use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use domain::artifact::ArtifactRef;
use domain::capture_session::{
    AdmittedTimer, AmbiguousEffect, AttemptIdentity, AuthorityError, BoundRecordSink, CloseLease,
    CloseLeaseReport, CloseOwnerRef, CloseState, CloseStorage, CommandKind, DispatchReport,
    FailureCause, HeartbeatPolicy, InputClass, MarkerState, ObservationClass, ObservationIdentity,
    PersistBoundaryError, PersistError, PersistErrorKind, QuiescenceReport, ReceiveStamp,
    RetentionBudget, ScopeBinding, SessionLifecycle, SessionTurn, SupervisorSessionHandle,
    TerminalFailure, TimerAdmission, TimerKind, TimerProgressView, WorkKind, WorkOwner,
};
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, PolicyFields, RecordingGate, SilenceRule, WatermarkKind};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;
use recording::*;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Default)]
struct AllocationSample {
    active: bool,
    live_requested_bytes: usize,
    peak_requested_bytes: usize,
    unmatched_deallocation: bool,
}

thread_local! {
    // Const TLS avoids allocation inside the allocator and isolates concurrently
    // running test threads. This measures requested heap bytes, not RSS or malloc
    // implementation bookkeeping.
    static ALLOCATION_SAMPLE: Cell<AllocationSample> = const { Cell::new(AllocationSample {
        active: false,
        live_requested_bytes: 0,
        peak_requested_bytes: 0,
        unmatched_deallocation: false,
    }) };
}

struct ObservedSystem;

fn allocation_change(old_size: usize, new_size: usize) {
    let _ = ALLOCATION_SAMPLE.try_with(|cell| {
        let mut sample = cell.get();
        if !sample.active {
            return;
        }
        if let Some(remaining) = sample.live_requested_bytes.checked_sub(old_size) {
            sample.live_requested_bytes = remaining + new_size;
            sample.peak_requested_bytes =
                sample.peak_requested_bytes.max(sample.live_requested_bytes);
        } else {
            sample.unmatched_deallocation = true;
        }
        cell.set(sample);
    });
}

// SAFETY: this test allocator forwards each allocation/deallocation unchanged
// to System. Counters contain no pointers, allocation or ownership behavior.
unsafe impl GlobalAlloc for ObservedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied GlobalAlloc's valid layout contract.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocation_change(0, layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded to System with the caller's unchanged layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocation_change(0, layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        allocation_change(layout.size(), 0);
        // SAFETY: pointer/layout are forwarded to their original allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: preserves System's original pointer/layout/reallocation rules.
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            allocation_change(layout.size(), new_size);
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: ObservedSystem = ObservedSystem;

struct AllocationProbe;

impl AllocationProbe {
    fn begin() -> Self {
        ALLOCATION_SAMPLE.with(|cell| {
            assert!(!cell.get().active);
            cell.set(AllocationSample {
                active: true,
                ..AllocationSample::default()
            });
        });
        Self
    }

    fn sample(&self) -> AllocationSample {
        ALLOCATION_SAMPLE.with(Cell::get)
    }
}

impl Drop for AllocationProbe {
    fn drop(&mut self) {
        ALLOCATION_SAMPLE.with(|cell| {
            let mut sample = cell.get();
            sample.active = false;
            cell.set(sample);
        });
    }
}

struct TempWal(PathBuf);

impl TempWal {
    fn new(label: &str) -> Self {
        let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "proscalping-session-{}-{label}-{serial}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

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

fn failed_marker() -> RecordFrame {
    frame(
        5,
        Record::Control(ControlRecord {
            context: context(700, false),
            value: Control::Recording(RecordingEvidence {
                health: RecordingHealth::Failed,
                kind: WatermarkKind::Written,
                through: Some(record(4)),
                reason: Reason::QueueOverflow,
            }),
        }),
    )
}

fn read_all(path: &PathBuf) -> (Vec<RecordFrame>, PhysicalReport) {
    let mut reader = WalReader::open(path).expect("read diagnostic prefix");
    let mut records = Vec::new();
    while let Ok(Some(frame)) = reader.next_record() {
        records.push(frame);
    }
    (records, reader.report().clone())
}

fn write_prefix(path: &PathBuf, marker: bool) {
    let mut writer = WalWriter::create(path).expect("fresh WAL");
    writer.append(&start()).expect("start");
    for frame in bootstrap() {
        writer.append(&frame).expect("bootstrap");
    }
    if marker {
        writer.append(&failed_marker()).expect("failure marker");
    }
    writer.flush().expect("expose physical prefix");
}

fn prefix_stats(frames: &[RecordFrame]) -> (u64, u64, u32) {
    let mut physical = 0;
    let mut crc = Crc32::default();
    for frame in frames {
        let bytes = encode_frame(frame).expect("encode accepted frame");
        physical += bytes.len() as u64;
        crc.update(&bytes[..bytes.len() - 4]);
    }
    (frames.len() as u64, physical, crc.digest())
}

fn segment_seal(frames: &[RecordFrame], final_segment: bool) -> RecordFrame {
    let (count, physical, crc) = prefix_stats(frames);
    let prior = frames.last().expect("nonempty prefix").record_no;
    frame(
        prior.get() + 1,
        Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: count,
            prefix_physical_len: physical,
            prefix_crc32: crc,
            prior_record: prior,
            has_gap: false,
            is_final: final_segment,
        }),
    )
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

fn owner_with_fault(
    path: &PathBuf,
    gate: RecordingGate,
    fault: Option<SinkFault>,
) -> (
    CaptureSessionOwner,
    SessionTurn,
    SupervisorSessionHandle,
    BoundRecordSink,
) {
    let mut definitions = bootstrap();
    let Record::ConfigDefinition(config) = &mut definitions.last_mut().expect("config").value
    else {
        unreachable!("fixture config")
    };
    config.fields.recording_gate = gate;
    let (mut owner, mut turn) =
        CaptureSessionOwner::create_new(path, &start(), BoundedCaptureProfile::new(&definitions))
            .expect("fresh bounded owner");
    let (handle, sink) = owner
        .register_supervisor_with_fault(&mut turn, &[scope()], budget(), fault)
        .expect("one accepted registry");
    (owner, turn, handle, sink)
}

fn owner(
    path: &PathBuf,
) -> (
    CaptureSessionOwner,
    SessionTurn,
    SupervisorSessionHandle,
    BoundRecordSink,
) {
    owner_with_fault(path, RecordingGate::Written, None)
}

fn owner_two_scopes(
    path: &PathBuf,
) -> (
    CaptureSessionOwner,
    SessionTurn,
    SupervisorSessionHandle,
    BoundRecordSink,
) {
    owner_two_scopes_with_gate(path, RecordingGate::Written)
}

fn owner_two_scopes_with_gate(
    path: &PathBuf,
    gate: RecordingGate,
) -> (
    CaptureSessionOwner,
    SessionTurn,
    SupervisorSessionHandle,
    BoundRecordSink,
) {
    let mut definitions = bootstrap();
    let mut config = definitions.pop().expect("config");
    config.record_no = record(6);
    let Record::ConfigDefinition(config_definition) = &mut config.value else {
        unreachable!("fixture config")
    };
    config_definition.fields.recording_gate = gate;
    config_definition.context = context(6, true);
    let mut second_spec = definitions[0].clone();
    second_spec.record_no = record(4);
    let Record::InstrumentSpec(second_spec_definition) = &mut second_spec.value else {
        unreachable!("fixture numeric definition")
    };
    second_spec_definition.context = context(4, true);
    second_spec_definition.slot = positive(InstrumentSlot::new(2));
    let mut numeric = second_spec_definition.numeric.fields().clone();
    numeric.reference.instrument.native_symbol = positive(Token::new("ETHUSDT"));
    numeric.quantity_unit = positive(Token::new("ETH"));
    numeric.base_asset = positive(Token::new("ETH"));
    let second_reference = numeric.reference.clone();
    second_spec_definition.numeric = NumericSpec::new(numeric).expect("second numeric spec");
    let mut second = definitions.last().expect("stream").clone();
    second.record_no = record(5);
    let Record::StreamDefinition(second_definition) = &mut second.value else {
        unreachable!("fixture stream")
    };
    second_definition.context = context(5, true);
    second_definition.binding.id = positive(StreamId::new(2));
    second_definition.binding.instrument_slot = positive(InstrumentSlot::new(2));
    second_definition.binding.spec = second_reference;
    second_definition.binding.connection_id = positive(ConnectionId::new(2));
    second_definition.binding.book_id = Some(positive(BookId::new(2)));
    let second_scope = ScopeBinding {
        stream: second_definition.binding.id,
        connection: second_definition.binding.connection_id,
        epoch: second_definition.binding.tag.connection,
    };
    definitions.push(second_spec);
    definitions.push(second);
    definitions.push(config);
    let (mut owner, mut turn) =
        CaptureSessionOwner::create_new(path, &start(), BoundedCaptureProfile::new(&definitions))
            .expect("fresh two-scope bounded owner");
    let (handle, sink) = owner
        .register_supervisor(
            &mut turn,
            &[scope(), second_scope],
            RetentionBudget {
                item_cap: 9,
                ..budget()
            },
            HeartbeatPolicy::SupervisorV2,
        )
        .expect("fixed two-scope registry");
    (owner, turn, handle, sink)
}

fn terminal() -> TerminalFailure {
    let binding = binding();
    TerminalFailure {
        stream: binding.id,
        connection: binding.connection_id,
        observed_tag: binding.tag,
        current_epoch: binding.tag.connection,
        context: active(),
        stamp: ReceiveStamp {
            unix_ns: 700,
            monotonic_ns: 700,
        },
        input_class: InputClass::Raw,
        attempt: AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(1))),
        cause: FailureCause::QueueOverflow,
    }
}

fn leased(report: CloseLeaseReport) -> CloseLease {
    match report {
        CloseLeaseReport::Leased(lease) => lease,
        other => panic!("expected exactly one lease: {other:?}"),
    }
}

fn close_state(owner: &CaptureSessionOwner, close: &CloseOwnerRef) -> Option<CloseState> {
    owner
        .outstanding_close_owners()
        .iter()
        .find(|entry| entry.owner == *close)
        .map(|entry| entry.state)
}

#[test]
fn healthy_owner_finalizes_only_after_one_borrowed_ticket_proof() {
    let wal = TempWal::new("owner-healthy");
    let (mut owner, mut turn, handle, _sink) = owner(&wal.0);
    let ticket = owner
        .begin_finalization(&mut turn)
        .expect("close admission");
    assert!(matches!(
        owner.begin_finalization(&mut turn),
        Err(OwnerError::Authority(AuthorityError::AlreadyClosing))
    ));
    let mut proof = match handle.quiesce(&mut turn, &ticket) {
        QuiescenceReport::Ready(proof) => proof,
        other => panic!("settled owner must quiesce: {other:?}"),
    };
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    let finalized = owner
        .finalize(&mut turn, &mut proof)
        .expect("canonical seals");
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    assert!(finalized.watermarks().durable.is_some());
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::Complete);
    assert_eq!(report.input_quality, Some(InputQuality::Unknown));
    assert!(matches!(
        records[records.len() - 2].value,
        Record::SegmentSeal(SegmentSeal { is_final: true, .. })
    ));
    assert!(matches!(
        records.last().expect("seal").value,
        Record::ArchiveSeal(_)
    ));
}

#[test]
fn foreign_finalize_preserves_same_genuine_proof_and_both_owners_until_rightful_use() {
    let wal_a = TempWal::new("proof-a");
    let wal_b = TempWal::new("proof-b");
    let (mut owner_a, mut turn_a, handle_a, _sink_a) = owner(&wal_a.0);
    let (mut owner_b, mut turn_b, handle_b, _sink_b) = owner(&wal_b.0);
    // Equal archive/session numeric identities do not authenticate an owner.
    assert_eq!(
        handle_a.authority().binding(),
        handle_b.authority().binding()
    );
    let ticket_a = owner_a.begin_finalization(&mut turn_a).unwrap();
    let ticket_b = owner_b.begin_finalization(&mut turn_b).unwrap();
    let QuiescenceReport::Ready(mut proof_a) = handle_a.quiesce(&mut turn_a, &ticket_a) else {
        panic!("A must issue its sole genuine proof")
    };
    let QuiescenceReport::Ready(mut proof_b) = handle_b.quiesce(&mut turn_b, &ticket_b) else {
        panic!("B must issue its sole genuine proof")
    };
    let state_a = owner_a.session_status();
    let state_b = owner_b.session_status();
    let ledger_a = handle_a.authority().ownership_report();
    let ledger_b = handle_b.authority().ownership_report();
    let watermarks_a = owner_a.watermarks();
    let watermarks_b = owner_b.watermarks();
    let bytes_a = fs::read(&wal_a.0).unwrap();
    let bytes_b = fs::read(&wal_b.0).unwrap();
    macro_rules! reject_without_mutation {
        ($attempt:expr) => {
            assert!(matches!(
                $attempt,
                Err(OwnerError::Authority(AuthorityError::AuthorityMismatch))
            ));
            assert_eq!(owner_a.session_status(), state_a);
            assert_eq!(owner_b.session_status(), state_b);
            assert_eq!(handle_a.authority().ownership_report(), ledger_a);
            assert_eq!(handle_b.authority().ownership_report(), ledger_b);
            assert_eq!(owner_a.watermarks(), watermarks_a);
            assert_eq!(owner_b.watermarks(), watermarks_b);
            assert_eq!(fs::read(&wal_a.0).unwrap(), bytes_a);
            assert_eq!(fs::read(&wal_b.0).unwrap(), bytes_b);
        };
    }
    reject_without_mutation!(owner_b.finalize(&mut turn_b, &mut proof_a));
    reject_without_mutation!(owner_a.finalize(&mut turn_b, &mut proof_a));
    reject_without_mutation!(owner_b.finalize(&mut turn_a, &mut proof_a));
    reject_without_mutation!(owner_a.finalize(&mut turn_a, &mut proof_b));
    reject_without_mutation!(owner_b.finalize(&mut turn_a, &mut proof_b));
    reject_without_mutation!(owner_a.finalize(&mut turn_b, &mut proof_b));
    assert!(matches!(
        handle_a.quiesce(&mut turn_a, &ticket_a),
        QuiescenceReport::TicketConsumed
    ));
    assert!(matches!(
        handle_b.quiesce(&mut turn_b, &ticket_b),
        QuiescenceReport::TicketConsumed
    ));
    let finalized_a = owner_a.finalize(&mut turn_a, &mut proof_a).unwrap();
    let finalized_b = owner_b.finalize(&mut turn_b, &mut proof_b).unwrap();
    assert!(finalized_a.watermarks().durable.is_some());
    assert!(finalized_b.watermarks().durable.is_some());
    for (owner, turn, proof, wal) in [
        (&mut owner_a, &mut turn_a, &mut proof_a, &wal_a),
        (&mut owner_b, &mut turn_b, &mut proof_b, &wal_b),
    ] {
        let final_bytes = fs::read(&wal.0).unwrap();
        assert!(matches!(
            owner.finalize(turn, proof),
            Err(OwnerError::Authority(AuthorityError::ProofConsumed))
        ));
        assert_eq!(fs::read(&wal.0).unwrap(), final_bytes);
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::Complete);
        assert_eq!(
            records
                .iter()
                .filter(|frame| matches!(
                    frame.value,
                    Record::SegmentSeal(SegmentSeal { is_final: true, .. })
                ))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|frame| matches!(frame.value, Record::ArchiveSeal(_)))
                .count(),
            1
        );
    }
}

#[test]
fn repeated_not_ready_preserves_ticket_and_counts_until_caller_settles_work() {
    let wal = TempWal::new("borrowed-not-ready");
    let (mut owner, mut turn, handle, _sink) = owner(&wal.0);
    let work = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .expect("counted pending owner");
    let ticket = owner
        .begin_finalization(&mut turn)
        .expect("healthy closing");
    let before = handle.authority().ownership_report();
    let mut first = None;
    for _ in 0..3 {
        let QuiescenceReport::NotReady(summary) = handle.quiesce(&mut turn, &ticket) else {
            panic!("borrowed ticket must remain NotReady")
        };
        assert_eq!(summary.work_total, 1);
        assert_eq!(summary.pending_plans, 1);
        if let Some(prior) = &first {
            assert_eq!(prior, &summary);
        } else {
            first = Some(summary);
        }
        assert_eq!(handle.authority().ownership_report(), before);
        assert_eq!(owner.session_status().lifecycle, SessionLifecycle::Closing);
    }
    drop(work);
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("same ticket must become ready after explicit settlement")
    };
    owner
        .finalize(&mut turn, &mut proof)
        .expect("sole proof accepted");
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
}

#[test]
fn failure_invalidates_issued_proof_without_any_final_seal() {
    let wal = TempWal::new("failed-issued-proof");
    let (mut owner, mut turn, handle, _sink) = owner(&wal.0);
    let ticket = owner
        .begin_finalization(&mut turn)
        .expect("close admission");
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("initially ready")
    };
    let termination = handle
        .terminate(&mut turn, terminal())
        .expect("latch failure");
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::DiagnosticClosing
    );
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
    ));
    assert!(matches!(
        owner.finalize(&mut turn, &mut proof),
        Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
    ));
    assert_eq!(owner.watermarks().written, Some(record(4)));
    drop(termination.close);
    drop(owner);
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(report.input_quality, None);
    assert!(!records.iter().any(|record| matches!(
        record.value,
        Record::SegmentSeal(_) | Record::ArchiveSeal(_)
    )));
}

#[test]
fn diagnostic_close_is_pollable_and_close_survives_descriptor_closure() {
    let wal = TempWal::new("diagnostic-close");
    let (mut owner, mut turn, handle, mut sink) = owner(&wal.0);
    assert!(matches!(
        owner.close_diagnostic(&mut turn).outcome,
        Err(OwnerError::Authority(AuthorityError::WrongLifecycle))
    ));
    let termination = handle
        .terminate(&mut turn, terminal())
        .expect("terminal scope");
    let close = termination.close_owner;
    drop(termination.close);
    let before = handle.authority().ownership_report();
    let report = owner.close_diagnostic(&mut turn);
    assert!(matches!(report.outcome, Ok(DiagnosticCloseState::Closing)));
    assert!(!report.physical_report.descriptor_closed);
    assert_eq!(report.input_completeness, InputQuality::Unknown);
    sink.persist(&mut turn, &failed_marker(), RecordingGate::Written)
        .expect("diagnostic marker receipt");
    handle
        .authority()
        .marker_confirmed(&mut turn, record(5))
        .expect("marker settled");
    let report = owner.close_diagnostic(&mut turn);
    assert!(matches!(report.outcome, Ok(DiagnosticCloseState::Closed)));
    assert!(report.physical_report.descriptor_closed);
    assert_eq!(report.outstanding_close_owners.iter().count(), 1);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    let mut after_descriptor_close = before;
    after_descriptor_close.storage_memory.backend_backing_bytes =
        owner.memory_report().backend_allocated_bytes;
    assert_eq!(
        before.storage_memory.backend_backing_bytes
            - after_descriptor_close.storage_memory.backend_backing_bytes,
        8192
    );
    assert_eq!(
        handle.authority().ownership_report(),
        after_descriptor_close
    );
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert_eq!(
        handle.authority().ownership_report(),
        after_descriptor_close
    );
    assert!(matches!(
        owner.dispatch(&mut turn, lease.into_command().unwrap(), |command| {
            assert_eq!(command.kind, &CommandKind::Close);
            assert_eq!(command.epoch, binding().tag.connection);
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(close_state(&owner, &close), None);
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::AlreadySettled
    ));
    assert!(matches!(
        owner.close_diagnostic(&mut turn).outcome,
        Ok(DiagnosticCloseState::Closed)
    ));
    let (records, report) = read_all(&wal.0);
    assert_eq!(records.last(), Some(&failed_marker()));
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(report.input_quality, None);
}

#[test]
fn down_close_drop_error_double_reclaim_and_terminal_reuse_preserve_owner() {
    let wal = TempWal::new("close-reclaim");
    let (mut owner, mut turn, handle, _sink) = owner(&wal.0);
    let work = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .expect("Down owner");
    let close = handle
        .mandatory_close(
            &mut turn,
            binding().id,
            binding().tag.connection,
            Some(&work),
        )
        .expect("same counted Down close");
    assert!(matches!(close.storage(), CloseStorage::WorkOwner(_)));
    let before = handle.authority().ownership_report();
    let first = leased(owner.reclaim_close(&mut turn, close.clone()));
    let leased_before = handle.authority().ownership_report();
    let alias_capacity =
        leased_before.inline_accounted_capacity_bytes - before.inline_accounted_capacity_bytes;
    assert_eq!(leased_before.work_references, before.work_references + 1);
    assert_eq!(
        leased_before.metadata_backing_bytes,
        before.metadata_backing_bytes
    );
    assert!(alias_capacity > 0 && alias_capacity <= 4096);
    assert!(leased_before.inline_accounted_capacity_bytes <= leased_before.inline_ceiling_bytes);
    assert!(leased_before.metadata_backing_bytes <= leased_before.metadata_ceiling_bytes);
    let mut expected_leased = before;
    expected_leased.work_references += 1;
    expected_leased.inline_accounted_capacity_bytes += alias_capacity;
    assert_eq!(
        leased_before, expected_leased,
        "Close lease shares exactly the same counted W owner"
    );
    assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
    assert!(matches!(
        owner.reclaim_close(&mut turn, close.clone()),
        CloseLeaseReport::AlreadyLeased
    ));
    assert_eq!(handle.authority().ownership_report(), leased_before);
    drop(first);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert_eq!(handle.authority().ownership_report(), before);
    let retry = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(&mut turn, retry.into_command().unwrap(), |_| Err::<(), _>(
            "ambiguous effect"
        )),
        DispatchReport::DispatchFailed {
            error: "ambiguous effect",
            effect: AmbiguousEffect::Unknown
        }
    ));
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert_eq!(handle.authority().ownership_report(), before);
    let held = leased(owner.reclaim_close(&mut turn, close.clone()));
    let terminal = handle
        .terminate(&mut turn, terminal())
        .expect("terminal reuse");
    assert_eq!(terminal.close_owner, close);
    assert!(terminal.close.is_none());
    assert_eq!(owner.outstanding_close_owners().iter().count(), 1);
    let after_cut = handle.authority().ownership_report();
    assert_eq!(after_cut.work_used, before.work_used);
    assert_eq!(after_cut.reserved_scopes, before.reserved_scopes);
    assert_eq!(after_cut.reserved_archive, before.reserved_archive);
    drop(held);
    let mut pending_after_cut = after_cut;
    pending_after_cut.work_references -= 1;
    pending_after_cut.inline_accounted_capacity_bytes -= alias_capacity;
    assert_eq!(handle.authority().ownership_report(), pending_after_cut);
    let retry = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            retry.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert_eq!(close_state(&owner, &close), None);
    assert_eq!(
        handle.authority().ownership_report().work_used,
        before.work_used
    );
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
}

#[test]
fn foreign_dispatch_returns_the_same_valid_close_lease_without_effect() {
    let wal_a = TempWal::new("close-authority-a");
    let wal_b = TempWal::new("close-authority-b");
    let (mut owner_a, mut turn_a, handle_a, _sink_a) = owner(&wal_a.0);
    let (mut owner_b, mut turn_b, _handle_b, _sink_b) = owner(&wal_b.0);
    let termination = handle_a
        .terminate(&mut turn_a, terminal())
        .expect("close a");
    let close = termination.close_owner;
    let command = termination
        .close
        .expect("initial close")
        .into_command()
        .unwrap();
    let before = handle_a.authority().ownership_report();
    assert!(matches!(
        owner_a.reclaim_close(&mut turn_b, close.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        owner_b.reclaim_close(&mut turn_b, close.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    let returned = match owner_b.dispatch(&mut turn_b, command, |_| -> Result<(), ()> {
        panic!("foreign authority must never invoke Close effect")
    }) {
        DispatchReport::Denied {
            reason: AuthorityError::AuthorityMismatch,
            command,
        } => command,
        other => panic!("must return rightful affine command: {other:?}"),
    };
    assert_eq!(returned.close_owner(), Some(&close));
    assert_eq!(close_state(&owner_a, &close), Some(CloseState::Leased));
    assert_eq!(handle_a.authority().ownership_report(), before);
    drop(returned);
    assert_eq!(close_state(&owner_a, &close), Some(CloseState::Pending));
    assert_eq!(handle_a.authority().ownership_report(), before);
}

#[test]
fn foreign_ticket_or_turn_cannot_consume_rightful_not_ready_ticket() {
    let wal_a = TempWal::new("ticket-a");
    let wal_b = TempWal::new("ticket-b");
    let (mut owner_a, mut turn_a, handle_a, _sink_a) = owner(&wal_a.0);
    let (mut owner_b, mut turn_b, _handle_b, _sink_b) = owner(&wal_b.0);
    let work = handle_a
        .reserve_work(&mut turn_a, WorkKind::Result)
        .expect("retained result");
    let ticket_a = owner_a.begin_finalization(&mut turn_a).expect("ticket a");
    let ticket_b = owner_b.begin_finalization(&mut turn_b).expect("ticket b");
    let before = handle_a.authority().ownership_report();
    assert!(matches!(
        handle_a.quiesce(&mut turn_b, &ticket_a),
        QuiescenceReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        handle_a.quiesce(&mut turn_a, &ticket_b),
        QuiescenceReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert_eq!(handle_a.authority().ownership_report(), before);
    assert!(matches!(
        handle_a.quiesce(&mut turn_a, &ticket_a),
        QuiescenceReport::NotReady(_)
    ));
    drop(work);
    let QuiescenceReport::Ready(mut proof) = handle_a.quiesce(&mut turn_a, &ticket_a) else {
        panic!("foreign errors must preserve rightful issuance")
    };
    owner_a
        .finalize(&mut turn_a, &mut proof)
        .expect("rightful sole proof");
}

#[test]
fn closing_keeps_down_close_reclaimable_until_explicit_settlement() {
    let wal = TempWal::new("closing-close");
    let (mut owner, mut turn, handle, _sink) = owner(&wal.0);
    let work = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .expect("admitted Down plan");
    let close = handle
        .mandatory_close(
            &mut turn,
            binding().id,
            binding().tag.connection,
            Some(&work),
        )
        .expect("same Down owner");
    let held = leased(owner.reclaim_close(&mut turn, close.clone()));
    let ticket = owner
        .begin_finalization(&mut turn)
        .expect("healthy Closing");
    let before = handle.authority().ownership_report();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(held);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    let reclaimed = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert_eq!(handle.authority().ownership_report(), before);
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            reclaimed.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert_eq!(
        handle.authority().ownership_report().work_used,
        before.work_used
    );
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(work);
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("same ticket must become ready after complete Down settlement")
    };
    owner
        .finalize(&mut turn, &mut proof)
        .expect("one physical finalization");
    assert_eq!(read_all(&wal.0).1.status, ArchiveStatus::Complete);
}

#[test]
fn fresh_owner_rejects_existing_archive_and_unbounded_or_nonbootstrap_profiles() {
    let existing = TempWal::new("no-adopt");
    let (owner, _turn, _handle, _sink) = owner(&existing.0);
    drop(owner);
    let definitions = bootstrap();
    assert!(matches!(
        CaptureSessionOwner::create_new(
            &existing.0,
            &start(),
            BoundedCaptureProfile::new(&definitions)
        ),
        Err(OwnerError::Create(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
    ));
    assert_eq!(read_all(&existing.0).1.last_record, Some(record(4)));

    let rejected = TempWal::new("bad-profile");
    let mut invalid = bootstrap();
    invalid[0].value = Record::RawInput(RawInput {
        context: context(2, false),
        stream: binding().id,
        tag: binding().tag,
        attempt: positive(CaptureAttemptNo::new(1)),
        bytes: vec![0],
    });
    assert!(matches!(
        CaptureSessionOwner::create_new(
            &rejected.0,
            &start(),
            BoundedCaptureProfile::new(&invalid)
        ),
        Err(OwnerError::InvalidProfile(_))
    ));
    assert!(!rejected.0.exists());
    let oversized_path = PathBuf::from("x".repeat(MAX_CAPTURE_PATH_BYTES + 1));
    assert!(matches!(
        CaptureSessionOwner::create_new(
            &oversized_path,
            &start(),
            BoundedCaptureProfile::new(&definitions)
        ),
        Err(OwnerError::InvalidProfile(_))
    ));
    assert!(matches!(
        CaptureSessionOwner::create_new(
            &rejected.0,
            &start(),
            BoundedCaptureProfile {
                bootstrap: &definitions,
                max_frame_len: 36
            }
        ),
        Err(OwnerError::InvalidProfile(_))
    ));
    assert!(!rejected.0.exists());
}

#[test]
fn marker_error_mismatch_and_weak_gate_stop_storage_without_promising_absent_bytes() {
    let error = PersistError::typed(PersistErrorKind::Io, "injected marker write");
    for (label, kind) in [
        ("error", SinkFaultKind::BeforeWrite(error)),
        (
            "mismatch",
            SinkFaultKind::ReceiptMismatch { through: record(4) },
        ),
        (
            "weak",
            SinkFaultKind::WeakGate {
                achieved: RecordingGate::Flushed,
            },
        ),
    ] {
        let wal = TempWal::new(label);
        let fault = SinkFault {
            at: record(5),
            kind,
        };
        let (mut owner, mut turn, handle, mut sink) =
            owner_with_fault(&wal.0, RecordingGate::Durable, Some(fault));
        let termination = handle
            .terminate(&mut turn, terminal())
            .expect("archive failure before marker");
        let close = termination.close_owner;
        drop(termination.close);
        let before = handle.authority().ownership_report();
        let mut marker = failed_marker();
        let Record::Control(ControlRecord {
            value: Control::Recording(evidence),
            ..
        }) = &mut marker.value
        else {
            unreachable!("fixture failure marker")
        };
        evidence.kind = WatermarkKind::Durable;
        evidence.through = handle.trusted_watermark(WatermarkKind::Durable);
        assert_eq!(
            evidence.through,
            Some(record(4)),
            "accepted bootstrap achieved Durable"
        );
        let actual = sink.persist(&mut turn, &marker, RecordingGate::Durable);
        match kind {
            SinkFaultKind::BeforeWrite(_) => {
                assert_eq!(actual, Err(PersistBoundaryError::Persistence(error)))
            }
            SinkFaultKind::ReceiptMismatch { through } => assert_eq!(
                actual,
                Err(PersistBoundaryError::ReceiptMismatch {
                    expected: record(5),
                    actual: through
                })
            ),
            SinkFaultKind::WeakGate { achieved } => assert_eq!(
                actual,
                Err(PersistBoundaryError::WeakGate {
                    required: RecordingGate::Durable,
                    achieved
                })
            ),
        }
        let status = owner.session_status();
        assert!(status.failed);
        assert!(status.storage_stopped.is_some());
        assert!(matches!(status.marker, MarkerState::Unconfirmed(_)));
        assert_eq!(status.first_failure, Some(terminal()));
        assert_eq!(handle.authority().ownership_report(), before);
        assert_eq!(
            sink.persist(&mut turn, &marker, RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::StorageStopped
            ))
        );
        assert!(matches!(
            owner.begin_finalization(&mut turn),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        let closed = owner.close_diagnostic(&mut turn);
        assert!(matches!(closed.outcome, Ok(DiagnosticCloseState::Closed)));
        assert!(closed.physical_report.unconfirmed_suffix_possible);
        let mut after_descriptor_close = before;
        after_descriptor_close.storage_memory.backend_backing_bytes =
            owner.memory_report().backend_allocated_bytes;
        assert_eq!(
            before.storage_memory.backend_backing_bytes
                - after_descriptor_close.storage_memory.backend_backing_bytes,
            8192
        );
        assert_eq!(
            handle.authority().ownership_report(),
            after_descriptor_close
        );
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert_eq!(
            handle.authority().ownership_report(),
            after_descriptor_close
        );
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert!(matches!(
            owner.reclaim_close(&mut turn, close),
            CloseLeaseReport::AlreadySettled
        ));
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert_eq!(report.input_quality, None);
        match kind {
            SinkFaultKind::BeforeWrite(_) => assert_eq!(records.len(), 4),
            SinkFaultKind::ReceiptMismatch { .. } | SinkFaultKind::WeakGate { .. } => {
                assert_eq!(records.last(), Some(&marker));
            }
        }
        assert!(!records.iter().any(|record| matches!(
            record.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        )));
    }
}

#[test]
fn bound_sink_rejects_final_seals_after_failure_before_writing_them() {
    let wal = TempWal::new("bound-seal-denial");
    let (mut owner, mut turn, handle, mut sink) = owner(&wal.0);
    let terminal = handle
        .terminate(&mut turn, terminal())
        .expect("failed archive");
    drop(terminal.close);
    let mut prefix = vec![start()];
    prefix.extend(bootstrap());
    let denied = sink.persist(
        &mut turn,
        &segment_seal(&prefix, true),
        RecordingGate::Written,
    );
    assert!(denied.is_err());
    assert_eq!(owner.watermarks().written, Some(record(4)));
    assert!(matches!(
        owner.begin_finalization(&mut turn),
        Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
    ));
    drop(owner);
    let (records, report) = read_all(&wal.0);
    assert_eq!(records, prefix);
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
}

#[test]
fn consuming_a_healthy_proof_does_not_authorize_generic_bound_sink_seals() {
    for archive_seal in [false, true] {
        let wal = TempWal::new("generic-seal-after-proof");
        let (mut owner, mut turn, handle, mut sink) = owner(&wal.0);
        let ticket = owner
            .begin_finalization(&mut turn)
            .expect("healthy Closing");
        let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("settled healthy session")
        };
        handle
            .authority()
            .consume_proof(&mut turn, &mut proof)
            .expect("affine authority proof");
        let mut prefix = vec![start()];
        prefix.extend(bootstrap());
        let candidate = if archive_seal {
            let (count, physical, crc) = prefix_stats(&prefix);
            frame(
                5,
                Record::ArchiveSeal(ArchiveSeal {
                    expected_segment_count: 1,
                    prior_frame_count: count,
                    total_prefix_physical_bytes: physical,
                    prefix_crc32: crc,
                    prior_record: record(4),
                    input_quality: InputQuality::Unknown,
                }),
            )
        } else {
            segment_seal(&prefix, true)
        };
        let denied = sink.persist(&mut turn, &candidate, RecordingGate::Written);
        assert!(matches!(
            denied,
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding | AuthorityError::InvalidOwner
            )) | Err(PersistBoundaryError::Persistence(PersistError {
                kind: PersistErrorKind::Authority,
                ..
            }))
        ));
        assert_eq!(owner.watermarks().written, Some(record(4)));
        let (records, report) = read_all(&wal.0);
        assert_eq!(records, prefix);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    }
}

#[test]
fn valid_failed_recording_observation_is_terminal_even_after_later_healthy_receipt() {
    for reason in [
        Reason::QueueOverflow,
        Reason::Unknown,
        Reason::WriteFailure,
        Reason::DecodeRejected,
    ] {
        let wal = TempWal::new("recorded-failure-latch");
        let (mut owner, mut turn, handle, mut sink) = owner(&wal.0);
        let mut failed = failed_marker();
        let Record::Control(ControlRecord {
            value: Control::Recording(evidence),
            ..
        }) = &mut failed.value
        else {
            unreachable!("fixture recording failure")
        };
        evidence.reason = reason;
        sink.persist_marker(&mut turn, &failed, RecordingGate::Written)
            .expect("valid archive-wide failed observation");
        handle
            .authority()
            .marker_confirmed(&mut turn, record(5))
            .expect("separately gate-confirmed marker");
        assert!(owner.session_status().failed);
        assert_eq!(owner.session_status().first_failure, None);
        assert_eq!(owner.session_status().storage_stopped, None);
        let healthy = frame(
            6,
            Record::Control(ControlRecord {
                context: context(701, false),
                value: Control::Recording(RecordingEvidence {
                    health: RecordingHealth::Healthy,
                    kind: WatermarkKind::Written,
                    through: Some(record(5)),
                    reason: Reason::NoFault,
                }),
            }),
        );
        let work = handle
            .reserve_work(&mut turn, WorkKind::InFlightObservation)
            .expect("counted later Healthy");
        sink.persist_owned(&mut turn, &healthy, RecordingGate::Written, &work)
            .expect("truthful later receipt");
        drop(work);
        assert_eq!(owner.watermarks().written, Some(record(6)));
        assert!(owner.session_status().failed);
        assert!(matches!(
            owner.begin_finalization(&mut turn),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        assert_eq!(handle.authority().terminal_failure(binding().id), None);
        let (records, report) = read_all(&wal.0);
        assert_eq!(records[4], failed);
        assert_eq!(records[5], healthy);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert_eq!(report.input_quality, None);
    }
}

#[test]
fn accepted_full_stream_binding_cannot_be_substituted_by_same_scope_ids() {
    let wal = TempWal::new("full-binding");
    let (_owner, _turn, handle, _sink) = owner(&wal.0);
    assert_eq!(handle.validate_stream_bindings(&[binding()]), Ok(()));
    let mut wrong_book = binding();
    wrong_book.book_id = Some(positive(BookId::new(2)));
    assert_eq!(
        handle.validate_stream_bindings(&[wrong_book]),
        Err(AuthorityError::InvalidBinding)
    );
    let mut wrong_tag = binding();
    wrong_tag.tag.subscription = positive(SubscriptionEpoch::new(2));
    assert_eq!(
        handle.validate_stream_bindings(&[wrong_tag]),
        Err(AuthorityError::InvalidBinding)
    );
    let mut wrong_profile = binding();
    wrong_profile.feed_profile = positive(FeedProfileVersion::new(2));
    assert_eq!(
        handle.validate_stream_bindings(&[wrong_profile]),
        Err(AuthorityError::InvalidBinding)
    );
    assert_eq!(handle.validate_stream_bindings(&[binding()]), Ok(()));
}

#[test]
fn bounded_profile_freezes_definitions_and_reports_storage_capacities() {
    let wal = TempWal::new("frozen-bootstrap");
    let (owner, mut turn, _handle, mut sink) = owner(&wal.0);
    let mut config = bootstrap().pop().expect("config definition");
    config.record_no = record(5);
    let Record::ConfigDefinition(definition) = &mut config.value else {
        unreachable!("fixture config")
    };
    definition.context = context(5, false);
    assert!(
        sink.persist(&mut turn, &config, RecordingGate::Written)
            .is_err()
    );
    assert_eq!(owner.watermarks().written, Some(record(4)));
    let memory = owner.memory_report();
    assert_eq!(memory.registry_record_count, 4);
    assert!(memory.registry_encoded_bytes <= MAX_BOOTSTRAP_BYTES);
    assert!(memory.registry_metadata_bound >= memory.registry_encoded_bytes);
    assert!(memory.encoder_workspace_bound >= memory.max_frame_len);
    assert!(memory.backend_allocated_bytes >= 8192);
}

#[test]
fn requested_allocation_peak_is_bounded_and_terminal_reclaim_does_not_grow_retention() {
    // The path predates tracking and is held until tracking ends. Everything
    // created inside the tracked window is dropped before the final sample.
    for cap in [5, 9] {
        let wal = TempWal::new("allocation-evidence");
        let probe = AllocationProbe::begin();
        let (mut owner, mut turn, handle, sink) = if cap == 5 {
            owner(&wal.0)
        } else {
            owner_two_scopes(&wal.0)
        };
        let metadata = owner.memory_report();
        let ledger = handle.authority().ownership_report();
        let computed_ceiling = ledger.metadata_ceiling_bytes
            + metadata.known_metadata_backing_bytes
            + metadata.registry_metadata_bound
            + metadata.encoder_workspace_bound
            + MAX_CAPTURE_PATH_BYTES
            + 8192;
        let constructed = probe.sample();
        assert!(!constructed.unmatched_deallocation);
        assert!(constructed.live_requested_bytes >= 8192);
        assert!(constructed.peak_requested_bytes <= computed_ceiling);

        let termination = handle
            .terminate(&mut turn, terminal())
            .expect("first failure");
        let close = termination.close_owner;
        drop(termination.close);
        let settled_failure_bytes = probe.sample().live_requested_bytes;
        let fixed_ledger = handle.authority().ownership_report();
        for _ in 0..100 {
            let repeated = handle
                .terminate(&mut turn, terminal())
                .expect("same fixed failure");
            assert_eq!(repeated.close_owner, close);
            drop(repeated.close);
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::AlreadyLeased
            ));
            drop(lease);
            assert_eq!(handle.authority().ownership_report(), fixed_ledger);
            assert_eq!(probe.sample().live_requested_bytes, settled_failure_bytes);
        }
        assert!(!probe.sample().unmatched_deallocation);
        assert!(probe.sample().peak_requested_bytes <= computed_ceiling);
        drop(close);
        drop(sink);
        drop(handle);
        drop(turn);
        drop(owner);
        assert_eq!(probe.sample().live_requested_bytes, 0);
        assert!(!probe.sample().unmatched_deallocation);
        let measured_peak = probe.sample().peak_requested_bytes;
        drop(probe);
        eprintln!(
            "capture allocation cap={cap}: constructed_live={} terminal_live={settled_failure_bytes} peak={measured_peak} computed_ceiling={computed_ceiling}",
            constructed.live_requested_bytes
        );
    }
}

#[test]
fn unsealed_prefix_before_marker_has_no_fabricated_failure_or_quality() {
    let wal = TempWal::new("before-marker");
    write_prefix(&wal.0, false);
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(report.input_quality, None);
    assert_eq!(report.last_record, Some(record(4)));
    assert_eq!(records.len(), 4);
    assert!(!records.iter().any(|frame| matches!(
        frame.value,
        Record::Control(ControlRecord {
            value: Control::Recording(_),
            ..
        })
    )));
}

#[test]
fn valid_marker_retains_archive_failure_without_inventing_missing_attempt() {
    let wal = TempWal::new("after-marker");
    write_prefix(&wal.0, true);
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(report.input_quality, None);
    assert_eq!(report.last_record, Some(record(5)));
    assert_eq!(records.last(), Some(&failed_marker()));
    assert!(
        !records
            .iter()
            .any(|frame| matches!(frame.value, Record::RawInput(_) | Record::Gap(_)))
    );
}

#[test]
fn torn_failure_marker_uses_existing_truncated_tail_semantics() {
    let wal = TempWal::new("torn-marker");
    write_prefix(&wal.0, true);
    let full = fs::read(&wal.0).expect("encoded prefix");
    let marker_len = encode_frame(&failed_marker()).expect("marker frame").len();
    let admitted_len = full.len() - marker_len;
    fs::write(&wal.0, &full[..full.len() - 1]).expect("crash torn byte");
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::TruncatedTail);
    assert_eq!(report.last_record, Some(record(4)));
    assert_eq!(report.physical_good_offset, admitted_len as u64);
    assert_eq!(records.len(), 4);
    assert_eq!(report.input_quality, None);
}

#[test]
fn old_nonfinal_segment_seal_remains_segment_sealed_archive_incomplete() {
    let wal = TempWal::new("old-segment-seal");
    let mut records = vec![start()];
    records.extend(bootstrap());
    records.push(failed_marker());
    records.push(segment_seal(&records, false));
    let mut writer = WalWriter::create(&wal.0).expect("legacy generic writer");
    for record in &records {
        writer.append(record).expect("accepted legacy prefix");
    }
    writer.flush().expect("physical prefix");
    let (_, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::SegmentSealedArchiveIncomplete);
    assert_eq!(report.input_quality, None);
}

#[test]
fn physically_valid_legacy_sealed_bytes_are_not_relabelled_by_failed_observation() {
    let wal = TempWal::new("legacy-complete");
    let mut records = vec![start()];
    records.extend(bootstrap());
    records.push(failed_marker());
    records.push(segment_seal(&records, true));
    let (count, physical, crc) = prefix_stats(&records);
    let prior = records.last().expect("segment seal").record_no;
    records.push(frame(
        prior.get() + 1,
        Record::ArchiveSeal(ArchiveSeal {
            expected_segment_count: 1,
            prior_frame_count: count,
            total_prefix_physical_bytes: physical,
            prefix_crc32: crc,
            prior_record: prior,
            input_quality: InputQuality::NoKnownLoss,
        }),
    ));
    let mut writer = WalWriter::create(&wal.0).expect("unbound legacy writer");
    for record in &records {
        writer
            .append(record)
            .expect("valid schema and physical chain");
    }
    writer.finish().expect("generic legacy finalization");
    let (recovered, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::Complete);
    assert_eq!(report.input_quality, Some(InputQuality::NoKnownLoss));
    assert_eq!(recovered, records);
    // This file is physically complete but violates the canonical capture-owner
    // policy. Recovery describes its bytes; it cannot erase a real ArchiveSeal.
}

// Independent QA, 2026-10-07. The four supplied normative test names are retained.
// Written is supplemental concrete-backend evidence; Durable counterparts keep
// the original Unix metadata requirement and are never weakened or ignored.
fn qa_original_identity(class: ObservationClass) -> ObservationIdentity {
    let bound = binding();
    let attempts = positive(CaptureAttemptNo::new(1));
    let raw_or_gap = matches!(
        class,
        ObservationClass::Raw | ObservationClass::RejectedStaleRaw | ObservationClass::Gap
    );
    ObservationIdentity {
        stream: bound.id,
        epoch: bound.tag.connection,
        stamp: ReceiveStamp {
            unix_ns: 5,
            monotonic_ns: 5,
        },
        class,
        tag: raw_or_gap.then_some(bound.tag),
        attempts: raw_or_gap.then_some((attempts, attempts)),
        loss_count: (class == ObservationClass::Gap).then_some(1),
    }
}

fn qa_context(identity: ObservationIdentity) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(identity.stamp.unix_ns),
        monotonic_ns: MonotonicNs::new(identity.stamp.monotonic_ns),
        context: InputContext::Active(active()),
    }
}

fn qa_raw(number: u64, identity: ObservationIdentity, empty: bool) -> RecordFrame {
    frame(
        number,
        Record::RawInput(RawInput {
            context: qa_context(identity),
            stream: identity.stream,
            tag: identity.tag.expect("original Raw tag"),
            attempt: identity.attempts.expect("original Raw attempt").0,
            bytes: if empty {
                Vec::new()
            } else {
                b"original received payload".to_vec()
            },
        }),
    )
}

fn qa_gap(number: u64, identity: ObservationIdentity, reason: Reason) -> RecordFrame {
    let queue_loss = identity.class == ObservationClass::Gap;
    frame(
        number,
        Record::Gap(Gap {
            context: qa_context(identity),
            scope: GapScope::ExplicitTargets(vec![GapTarget {
                stream: identity.stream,
                tag: identity.tag.expect("original GAP target tag"),
                range: if queue_loss { identity.attempts } else { None },
                loss_count: if queue_loss {
                    identity.loss_count
                } else {
                    None
                },
            }]),
            reason,
        }),
    )
}

fn qa_admit(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    identity: ObservationIdentity,
) -> WorkOwner {
    let work = handle
        .reserve_work(turn, WorkKind::QueuedObservation)
        .unwrap();
    handle.admit_observation(turn, &work, identity).unwrap();
    work.set_kind(turn, WorkKind::InFlightObservation).unwrap();
    work
}

fn qa_reject_preserving(
    boundary: (&TempWal, &CaptureSessionOwner, &SupervisorSessionHandle),
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    work: &WorkOwner,
    gate: RecordingGate,
    submitted: &RecordFrame,
) {
    let (wal, owner, handle) = boundary;
    let ledger = handle.authority().ownership_report();
    let status = owner.session_status();
    let prefix = handle.prefix();
    let close = owner.outstanding_close_owners();
    let watermarks = owner.watermarks();
    let physical = fs::read(&wal.0).unwrap();
    let backend_calls = owner.sink_persist_calls();
    let rejection = sink.persist_owned(turn, submitted, gate, work);
    let tokenless_timer = matches!(
        submitted.value,
        Record::Control(ControlRecord {
            value: Control::Timer { .. },
            ..
        })
    ) && handle.timer_progress(turn, work).is_err();
    assert!(
        matches!(
            rejection,
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding | AuthorityError::InvalidOwner
            ))
        ) || (tokenless_timer
            && rejection
                == Err(PersistBoundaryError::Authority(
                    AuthorityError::TimerAuthorityRequired
                ))),
        "metadata/stage substitution must reject before backend write: {submitted:?}"
    );
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(owner.session_status(), status);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.outstanding_close_owners(), close);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
    assert_eq!(owner.sink_persist_calls(), backend_calls);
}

fn qa_settle_once(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &BoundRecordSink,
    work: &WorkOwner,
) {
    handle.complete_observation(turn, sink, work, None).unwrap();
    let settled = handle.authority().ownership_report();
    assert_eq!(
        handle.complete_observation(turn, sink, work, None),
        Err(AuthorityError::InvalidOwner)
    );
    assert_eq!(handle.authority().ownership_report(), settled);
}

fn independent_qa_raw_receipt_probe(gate: RecordingGate, substitution: u8) {
    let wal = TempWal::new("independent-unrelated-raw");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let original = qa_raw(5, identity, false);
    let submitted = if substitution == 1 {
        frame(
            5,
            Record::Control(ControlRecord {
                context: context(5, false),
                value: Control::Timer {
                    stream: identity.stream,
                    timer_id: 99,
                    deadline_ns: 5,
                },
            }),
        )
    } else if substitution == 2 {
        let mut replacement = original.clone();
        let Record::RawInput(raw) = &mut replacement.value else {
            unreachable!()
        };
        raw.context = context(6, false);
        replacement
    } else {
        original.clone()
    };
    println!("gate={gate:?}; substitution={substitution}");
    if substitution != 0 {
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &submitted,
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        let before = handle.authority().ownership_report();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        assert_eq!(handle.authority().ownership_report(), before);
        assert_eq!(read_all(&wal.0).0.len(), 4);
        sink.persist_owned(&mut turn, &original, gate, &work)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &work);
        drop(work);
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::Ready(_)
        ));
    } else {
        sink.persist_owned(&mut turn, &submitted, gate, &work)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &work);
        drop(work);
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::Ready(_)
        ));
    }
    let (records, report) = read_all(&wal.0);
    assert_eq!(records.len(), 5);
    assert_eq!(records.last(), Some(&original));
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
}

#[test]
fn independent_qa_unrelated_authenticated_timer_does_not_settle_received_raw() {
    independent_qa_raw_receipt_probe(RecordingGate::Written, 1);
}

#[test]
fn independent_qa_replacement_stamp_does_not_settle_original_received_raw() {
    independent_qa_raw_receipt_probe(RecordingGate::Written, 2);
}

#[test]
fn independent_qa_matching_authenticated_raw_settles_exact_original_obligation() {
    independent_qa_raw_receipt_probe(RecordingGate::Written, 0);
}

#[test]
fn independent_qa_original_durable_profile_unrelated_timer_probe() {
    independent_qa_raw_receipt_probe(RecordingGate::Durable, 1);
}

fn qa_raw_metadata_family(gate: RecordingGate) {
    // Twelve substitutions; each retains one rightful Raw and a separate result W.
    for variant in 0..12 {
        let wal = TempWal::new("qa-new-raw-metadata");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let identity = qa_original_identity(ObservationClass::Raw);
        let work = qa_admit(&handle, &mut turn, identity);
        let protected = handle.reserve_work(&mut turn, WorkKind::Result).unwrap();
        let original = qa_raw(5, identity, false);
        let mut wrong = original.clone();
        let Record::RawInput(raw) = &mut wrong.value else {
            unreachable!()
        };
        match variant {
            0 => raw.context.unix_ns = LocalUnixNs::new(6),
            1 => raw.context.monotonic_ns = MonotonicNs::new(6),
            2 => raw.stream = positive(StreamId::new(2)),
            3 => raw.tag.connection = positive(ConnectionEpoch::new(2)),
            4 => raw.tag.spec = positive(SpecVersion::new(2)),
            5 => raw.tag.subscription = positive(SubscriptionEpoch::new(2)),
            6 => raw.tag.book = Some(positive(BookEpoch::new(2))),
            7 => raw.tag.book = None,
            8 => raw.attempt = positive(CaptureAttemptNo::new(2)),
            9 => raw.context.context = InputContext::Bootstrap,
            10 => wrong = qa_gap(5, identity, Reason::Unknown),
            11 => {
                wrong = frame(
                    5,
                    Record::Control(ControlRecord {
                        context: qa_context(identity),
                        value: Control::Timer {
                            stream: identity.stream,
                            timer_id: 9,
                            deadline_ns: 5,
                        },
                    }),
                )
            }
            _ => unreachable!(),
        }
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong,
        );
        assert_eq!(
            handle.authority().ownership_report().pending_observations,
            1
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
        sink.persist_owned(&mut turn, &original, gate, &work)
            .unwrap();
        let mut duplicate = original.clone();
        duplicate.record_no = record(6);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &duplicate,
        );
        qa_settle_once(&handle, &mut turn, &sink, &work);
        assert_eq!(handle.authority().ownership_report().work_used, 2);
        assert_eq!(
            handle.authority().ownership_report().pending_observations,
            0
        );
        drop(work);
        assert_eq!(handle.authority().ownership_report().work_used, 1);
        drop(protected);
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::Ready(_)
        ));
        assert_eq!(read_all(&wal.0).0.last(), Some(&original));
        assert_eq!(read_all(&wal.0).0.len(), 5);
    }
}

#[test]
fn qa_new_written_raw_metadata_rejection_preserves_rightful_once_only_completion() {
    qa_raw_metadata_family(RecordingGate::Written);
}

#[test]
fn qa_new_durable_raw_metadata_rejection_preserves_rightful_once_only_completion() {
    qa_raw_metadata_family(RecordingGate::Durable);
}

fn qa_missing_original_abandonment(gate: RecordingGate) {
    let wal = TempWal::new("qa-new-missing-original");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let work_id = work.id();
    let mut wrong = qa_raw(5, identity, false);
    let Record::RawInput(raw) = &mut wrong.value else {
        unreachable!()
    };
    raw.context.unix_ns = LocalUnixNs::new(6);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &wrong,
    );
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let before = handle.authority().ownership_report();
    let before_bytes = fs::read(&wal.0).unwrap();
    for _ in 0..3 {
        let QuiescenceReport::NotReady(summary) = handle.quiesce(&mut turn, &ticket) else {
            panic!("original Pending obligation must deny proof")
        };
        assert_eq!(summary.work_total, 1);
        assert_eq!(summary.in_flight, 1);
        assert_eq!(handle.authority().ownership_report(), before);
    }
    drop(work);
    let abandoned = handle.authority().ownership_report();
    assert_eq!(abandoned.work_used, 1);
    assert_eq!(abandoned.abandoned_work, 1);
    assert_eq!(abandoned.pending_observations, 0);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
    ));
    let status = owner.session_status();
    assert_eq!(
        status.storage_stopped.unwrap().kind,
        PersistErrorKind::OwnershipAbandoned
    );
    let lost = status.first_abandonment.unwrap();
    assert_eq!(lost.work_id, work_id);
    assert_eq!(lost.identity, identity);
    assert_eq!(lost.kind, WorkKind::InFlightObservation);
    assert_eq!(lost.cut_side, domain::capture_session::CutSide::PreCut);
    let close_snapshot = owner.outstanding_close_owners();
    let close = close_snapshot
        .iter()
        .next()
        .expect("abandonment retains mandatory Close");
    assert_eq!(close_snapshot.iter().count(), 1);
    assert_eq!(close.owner.stream(), identity.stream);
    assert_eq!(close.owner.epoch(), identity.epoch);
    assert_eq!(close.owner.storage(), CloseStorage::ReservedTerminal);
    assert_eq!(close.state, CloseState::Pending);
    let reconciled = handle.authority().ownership_report();
    for _ in 0..3 {
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
        ));
        assert_eq!(owner.session_status(), status);
        assert_eq!(owner.outstanding_close_owners(), close_snapshot);
        assert_eq!(handle.authority().ownership_report(), reconciled);
    }
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    assert_eq!(owner.watermarks().written, Some(record(4)));
    assert_eq!(fs::read(&wal.0).unwrap(), before_bytes);
    let (records, report) = read_all(&wal.0);
    assert_eq!(records.len(), 4);
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(report.input_quality, None);
    assert!(
        !records
            .iter()
            .any(|frame| matches!(frame.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
    );
}

#[test]
fn qa_new_written_missing_original_retains_abandoned_identity_and_denies_seals() {
    qa_missing_original_abandonment(RecordingGate::Written);
}

#[test]
fn qa_new_durable_missing_original_retains_abandoned_identity_and_denies_seals() {
    qa_missing_original_abandonment(RecordingGate::Durable);
}

fn qa_stale_two_stage(gate: RecordingGate) {
    let wal = TempWal::new("qa-new-stale-stages");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::RejectedStaleRaw);
    let work = qa_admit(&handle, &mut turn, identity);
    let gap_first = qa_gap(5, identity, Reason::Unknown);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &gap_first,
    );
    let nonempty = qa_raw(5, identity, false);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &nonempty,
    );
    let original = qa_raw(5, identity, true);
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    let duplicate = qa_raw(6, identity, true);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &duplicate,
    );
    for variant in 0..4 {
        let mut wrong = qa_gap(6, identity, Reason::Unknown);
        let Record::Gap(gap) = &mut wrong.value else {
            unreachable!()
        };
        match variant {
            0 => gap.reason = Reason::DecodeRejected,
            1 => gap.context.monotonic_ns = MonotonicNs::new(6),
            2 => {
                let GapScope::ExplicitTargets(targets) = &mut gap.scope else {
                    unreachable!()
                };
                targets[0].tag.subscription = positive(SubscriptionEpoch::new(2));
            }
            3 => gap.scope = GapScope::AllDeclaredStreams,
            _ => unreachable!(),
        }
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong,
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
    }
    let diagnostic = qa_gap(6, identity, Reason::Unknown);
    sink.persist_owned(&mut turn, &diagnostic, gate, &work)
        .unwrap();
    let mut duplicate_gap = diagnostic.clone();
    duplicate_gap.record_no = record(7);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &duplicate_gap,
    );
    qa_settle_once(&handle, &mut turn, &sink, &work);
    assert_eq!(
        handle.authority().ownership_report().pending_observations,
        0
    );
    assert_eq!(&read_all(&wal.0).0[4..], &[original, diagnostic]);
}

#[test]
fn qa_new_written_stale_raw_requires_distinct_exact_empty_raw_and_diagnostic_gap() {
    qa_stale_two_stage(RecordingGate::Written);
}

#[test]
fn qa_new_durable_stale_raw_requires_distinct_exact_empty_raw_and_diagnostic_gap() {
    qa_stale_two_stage(RecordingGate::Durable);
}

fn qa_queue_gap_metadata(gate: RecordingGate) {
    let wal = TempWal::new("qa-new-gap-identity");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let first = positive(CaptureAttemptNo::new(1));
    let mut identity = qa_original_identity(ObservationClass::Gap);
    let work = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    handle
        .admit_observation(&mut turn, &work, identity)
        .unwrap();
    identity.attempts = Some((first, positive(CaptureAttemptNo::new(3))));
    identity.loss_count = Some(3);
    handle
        .extend_gap_observation(&mut turn, &work, identity)
        .unwrap();
    work.set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    let original = qa_gap(5, identity, Reason::QueueOverflow);
    // Thirteen substitutions against the coalesced original range/stamp.
    for variant in 0..13 {
        let mut wrong = original.clone();
        let Record::Gap(gap) = &mut wrong.value else {
            unreachable!()
        };
        let GapScope::ExplicitTargets(targets) = &mut gap.scope else {
            unreachable!()
        };
        match variant {
            0 => gap.context.unix_ns = LocalUnixNs::new(6),
            1 => gap.context.monotonic_ns = MonotonicNs::new(6),
            2 => targets[0].stream = positive(StreamId::new(2)),
            3 => targets[0].tag.connection = positive(ConnectionEpoch::new(2)),
            4 => targets[0].tag.spec = positive(SpecVersion::new(2)),
            5 => targets[0].tag.subscription = positive(SubscriptionEpoch::new(2)),
            6 => targets[0].tag.book = Some(positive(BookEpoch::new(2))),
            7 => {
                targets[0].range = Some((
                    positive(CaptureAttemptNo::new(2)),
                    positive(CaptureAttemptNo::new(4)),
                ))
            }
            8 => {
                targets[0].range = Some((first, positive(CaptureAttemptNo::new(4))));
                targets[0].loss_count = Some(4);
            }
            9 => targets[0].loss_count = Some(2),
            10 => gap.scope = GapScope::AllDeclaredStreams,
            11 => {
                let mut extra = targets[0].clone();
                extra.stream = positive(StreamId::new(2));
                targets.push(extra);
            }
            12 => {
                gap.reason = Reason::Unknown;
                targets[0].range = None;
                targets[0].loss_count = None;
            }
            _ => unreachable!(),
        }
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong,
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
    }
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    let before = handle.authority().ownership_report();
    let mut later_identity = identity;
    later_identity.attempts = Some((first, positive(CaptureAttemptNo::new(4))));
    later_identity.loss_count = Some(4);
    assert_eq!(
        handle.extend_gap_observation(&mut turn, &work, later_identity),
        Err(AuthorityError::InvalidOwner)
    );
    assert_eq!(handle.authority().ownership_report(), before);
    let mut duplicate = original.clone();
    duplicate.record_no = record(6);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &duplicate,
    );
    qa_settle_once(&handle, &mut turn, &sink, &work);
    assert_eq!(read_all(&wal.0).0.last(), Some(&original));
    assert_eq!(read_all(&wal.0).0.len(), 5);
}

#[test]
fn qa_new_written_queue_gap_requires_original_target_range_count_stamp_and_stage() {
    qa_queue_gap_metadata(RecordingGate::Written);
}

#[test]
fn qa_new_durable_queue_gap_requires_original_target_range_count_stamp_and_stage() {
    qa_queue_gap_metadata(RecordingGate::Durable);
}

fn qa_raw_diagnostic_controls(gate: RecordingGate) {
    for reason in [Reason::Unknown, Reason::DecodeRejected, Reason::SourceGap] {
        let wal = TempWal::new("qa-new-raw-diagnostic");
        let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let identity = qa_original_identity(ObservationClass::Raw);
        let work = qa_admit(&handle, &mut turn, identity);
        let primary = qa_raw(5, identity, false);
        let before_primary = qa_gap(5, identity, reason);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &before_primary,
        );
        sink.persist_owned(&mut turn, &primary, gate, &work)
            .unwrap();
        let wrong_reason = qa_gap(6, identity, Reason::Reconnect);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong_reason,
        );
        let mut wrong_target = qa_gap(6, identity, reason);
        let Record::Gap(gap) = &mut wrong_target.value else {
            unreachable!()
        };
        let GapScope::ExplicitTargets(targets) = &mut gap.scope else {
            unreachable!()
        };
        targets[0].tag.book = Some(positive(BookEpoch::new(2)));
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong_target,
        );
        let diagnostic = qa_gap(6, identity, reason);
        sink.persist_owned(&mut turn, &diagnostic, gate, &work)
            .unwrap();
        let mut repeated = diagnostic.clone();
        repeated.record_no = record(7);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &repeated,
        );
        qa_settle_once(&handle, &mut turn, &sink, &work);
        assert_eq!(&read_all(&wal.0).0[4..], &[primary, diagnostic]);
    }
}

#[test]
fn qa_new_written_raw_allows_one_exact_same_observation_diagnostic_gap() {
    qa_raw_diagnostic_controls(RecordingGate::Written);
}

#[test]
fn qa_new_durable_raw_allows_one_exact_same_observation_diagnostic_gap() {
    qa_raw_diagnostic_controls(RecordingGate::Durable);
}

fn qa_transport(number: u64, identity: ObservationIdentity, value: Transport) -> RecordFrame {
    frame(
        number,
        Record::Control(ControlRecord {
            context: qa_context(identity),
            value: Control::Transport {
                connection: binding().connection_id,
                epoch: identity.epoch,
                value,
            },
        }),
    )
}

fn qa_received_control_metadata(gate: RecordingGate) {
    for class in [
        ObservationClass::Connected,
        ObservationClass::Pong,
        ObservationClass::Disconnected,
    ] {
        let wal = TempWal::new("qa-new-control-identity");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let identity = qa_original_identity(class);
        let work = qa_admit(&handle, &mut turn, identity);
        let value = if class == ObservationClass::Disconnected {
            Transport::Down
        } else {
            Transport::Up
        };
        let original = qa_transport(5, identity, value);
        for variant in 0..4 {
            let mut wrong = original.clone();
            let Record::Control(control) = &mut wrong.value else {
                unreachable!()
            };
            let Control::Transport {
                connection,
                epoch,
                value,
            } = &mut control.value
            else {
                unreachable!()
            };
            match variant {
                0 => control.context.unix_ns = LocalUnixNs::new(6),
                1 => *connection = positive(ConnectionId::new(2)),
                2 => *epoch = positive(ConnectionEpoch::new(2)),
                3 => *value = Transport::Unknown,
                _ => unreachable!(),
            }
            qa_reject_preserving(
                (&wal, &owner, &handle),
                &mut turn,
                &mut sink,
                &work,
                gate,
                &wrong,
            );
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, &work, None),
                Err(AuthorityError::NotQuiescent)
            );
        }
        sink.persist_owned(&mut turn, &original, gate, &work)
            .unwrap();
        let mut duplicate = original.clone();
        duplicate.record_no = record(6);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &duplicate,
        );
        qa_settle_once(&handle, &mut turn, &sink, &work);
        let close = (class == ObservationClass::Disconnected)
            .then(|| qa_pending_down_close(&owner, identity, &work));
        if let Some(close) = close {
            assert_eq!(close.storage(), CloseStorage::WorkOwner(work.id()));
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            drop(lease);
            assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
            assert_eq!(handle.authority().ownership_report().work_used, 1);
            let lease = leased(owner.reclaim_close(&mut turn, close));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
        }
        drop(work);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        assert_eq!(read_all(&wal.0).0.last(), Some(&original));
    }
}

#[test]
fn qa_new_written_received_controls_require_original_up_or_down_identity() {
    qa_received_control_metadata(RecordingGate::Written);
}

#[test]
fn qa_new_durable_received_controls_require_original_up_or_down_identity() {
    qa_received_control_metadata(RecordingGate::Durable);
}

fn qa_obsolete_control_no_write(gate: RecordingGate) {
    for class in [ObservationClass::Connected, ObservationClass::Pong] {
        let wal = TempWal::new("qa-new-obsolete-control");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let down_identity = qa_original_identity(ObservationClass::Disconnected);
        let down = qa_admit(&handle, &mut turn, down_identity);
        sink.persist_owned(
            &mut turn,
            &qa_transport(5, down_identity, Transport::Down),
            gate,
            &down,
        )
        .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &down);
        let close = qa_pending_down_close(&owner, down_identity, &down);
        let mut identity = qa_original_identity(class);
        identity.stamp = ReceiveStamp {
            unix_ns: 6,
            monotonic_ns: 6,
        };
        let work = qa_admit(&handle, &mut turn, identity);
        let before = handle.authority().ownership_report();
        let bytes = fs::read(&wal.0).unwrap();
        let invented_up = qa_transport(6, identity, Transport::Up);
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &invented_up,
        );
        assert_eq!(
            handle.complete_observation(
                &mut turn,
                &sink,
                &work,
                Some((identity.stream, positive(ConnectionEpoch::new(2))))
            ),
            Err(AuthorityError::NotQuiescent)
        );
        assert_eq!(handle.authority().ownership_report(), before);
        handle
            .complete_observation(
                &mut turn,
                &sink,
                &work,
                Some((identity.stream, identity.epoch)),
            )
            .unwrap();
        assert_eq!(
            handle.complete_observation(
                &mut turn,
                &sink,
                &work,
                Some((identity.stream, identity.epoch))
            ),
            Err(AuthorityError::InvalidOwner)
        );
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        assert_eq!(owner.watermarks().written, Some(record(5)));
        drop(work);
        drop(down);
        assert_eq!(handle.authority().ownership_report().work_used, 1);
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        let lease = leased(owner.reclaim_close(&mut turn, close));
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::Ready(_)
        ));
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        assert_eq!(read_all(&wal.0).0.len(), 5);
    }
}

#[test]
fn qa_new_written_authenticated_obsolete_up_and_pong_settle_without_new_write() {
    qa_obsolete_control_no_write(RecordingGate::Written);
}

#[test]
fn qa_new_durable_authenticated_obsolete_up_and_pong_settle_without_new_write() {
    qa_obsolete_control_no_write(RecordingGate::Durable);
}

#[test]
fn qa_new_durable_rightful_raw_after_rejection_finalizes_once_with_physical_complete() {
    let wal = TempWal::new("qa-new-healthy-finalization");
    let (mut owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let original = qa_raw(5, identity, false);
    let mut wrong = original.clone();
    let Record::RawInput(raw) = &mut wrong.value else {
        unreachable!()
    };
    raw.context.monotonic_ns = MonotonicNs::new(6);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        RecordingGate::Durable,
        &wrong,
    );
    sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    drop(work);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("healthy exact original")
    };
    owner.finalize(&mut turn, &mut proof).unwrap();
    let (records, report) = read_all(&wal.0);
    assert_eq!(report.status, ArchiveStatus::Complete);
    assert_eq!(records.len(), 7);
    assert_eq!(records[4], original);
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
    let finalized_bytes = fs::read(&wal.0).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    assert!(owner.finalize(&mut turn, &mut proof).is_err());
    assert_eq!(fs::read(&wal.0).unwrap(), finalized_bytes);
    assert_eq!(read_all(&wal.0).0, records);
}

fn qa_timer_identity_only(gate: RecordingGate) {
    // The old identity probes now exercise the authority's genuine due Ping;
    // authenticated Up is an additional, explicitly checked setup record.
    let wal = TempWal::new("qa-new-timer-identity");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    let admitted = qa_timer_due(&handle, &mut turn, binding().id, 30_000_000_005);
    assert_eq!(admitted.kind(), TimerKind::Ping);
    let identity = admitted.identity();
    let work = admitted.into_owner();
    let original = qa_timer_frame(6, identity);
    for variant in 0..6 {
        let mut wrong = original.clone();
        let Record::Control(control) = &mut wrong.value else {
            unreachable!()
        };
        let Control::Timer {
            stream,
            timer_id,
            deadline_ns,
        } = &mut control.value
        else {
            unreachable!()
        };
        match variant {
            0 => *timer_id += 1,
            1 => *deadline_ns -= 1,
            2 => control.context.unix_ns = LocalUnixNs::new(identity.stamp.unix_ns + 1),
            3 => control.context.monotonic_ns = MonotonicNs::new(identity.stamp.monotonic_ns + 1),
            4 => *stream = positive(StreamId::new(2)),
            5 => {
                control.value = Control::Transport {
                    connection: binding().connection_id,
                    epoch: identity.epoch,
                    value: Transport::Up,
                }
            }
            _ => unreachable!(),
        }
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong,
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
    }
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    let mut repeated = original.clone();
    repeated.record_no = record(7);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &repeated,
    );
    qa_settle_once(&handle, &mut turn, &sink, &work);
    let ping = handle.take_timer_ping(&mut turn, &work).unwrap();
    drop(ping);
    assert_eq!(read_all(&wal.0).0.len(), 6);
    assert!(matches!(
        read_all(&wal.0).0[4].value,
        Record::Control(ControlRecord {
            value: Control::Transport {
                value: Transport::Up,
                ..
            },
            ..
        })
    ));
    assert_eq!(read_all(&wal.0).0.last(), Some(&original));
}

#[test]
fn qa_new_written_timer_requires_original_token_deadline_scope_stamp_and_one_stage() {
    qa_timer_identity_only(RecordingGate::Written);
}

#[test]
fn qa_new_durable_timer_requires_original_token_deadline_scope_stamp_and_one_stage() {
    qa_timer_identity_only(RecordingGate::Durable);
}

fn qa_rejection_with_failure_cut_and_close(gate: RecordingGate) {
    let wal = TempWal::new("qa-new-cut-close-preservation");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let mut failure = terminal();
    failure.attempt = AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2)));
    let termination = handle.terminate(&mut turn, failure).unwrap();
    let close = termination.close_owner;
    drop(termination.close);
    assert_eq!(work.cut_side(), domain::capture_session::CutSide::PreCut);
    let held_close = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
    let before = owner.session_status();
    let original = qa_raw(5, identity, false);
    let mut wrong = original.clone();
    let Record::RawInput(raw) = &mut wrong.value else {
        unreachable!()
    };
    raw.context.unix_ns = LocalUnixNs::new(6);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &wrong,
    );
    assert_eq!(owner.session_status().first_failure, Some(failure));
    assert_eq!(owner.session_status().cut_sequence, before.cut_sequence);
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
    drop(held_close);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert!(matches!(
        owner.begin_finalization(&mut turn),
        Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
    ));
    let (records, report) = read_all(&wal.0);
    assert_eq!(records.len(), 5);
    assert_eq!(records.last(), Some(&original));
    assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert!(
        !records
            .iter()
            .any(|frame| matches!(frame.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
    );
}

#[test]
fn qa_new_written_rejected_raw_preserves_fixed_pre_cut_and_held_close_identity() {
    qa_rejection_with_failure_cut_and_close(RecordingGate::Written);
}

#[test]
fn qa_new_durable_rejected_raw_preserves_fixed_pre_cut_and_held_close_identity() {
    qa_rejection_with_failure_cut_and_close(RecordingGate::Durable);
}

fn qa_epoch(number: u64, identity: ObservationIdentity, change: EpochChange) -> RecordFrame {
    frame(
        number,
        Record::Control(ControlRecord {
            context: qa_context(identity),
            value: Control::EpochAdvance {
                change,
                reason: Reason::Reconnect,
            },
        }),
    )
}

fn qa_generated_original_stages(gate: RecordingGate) {
    let wal = TempWal::new("qa-new-generated-original-stages");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let bound = binding();
    let work = qa_admit(&handle, &mut turn, identity);
    let alias = work.share().unwrap();
    let down = qa_transport(5, identity, Transport::Down);
    sink.persist_owned(&mut turn, &down, gate, &work).unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    let close = qa_pending_down_close(&owner, identity, &work);
    work.set_kind(&mut turn, WorkKind::PendingPlan).unwrap();
    handle.retain_generated_plan(&mut turn, &work).unwrap();
    // The earlier authenticated Down is not fresh generated progress.
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    let connection = EpochChange::Connection {
        owner: bound.connection_id,
        expected: bound.tag.connection,
        next: bound.tag.connection.checked_next().unwrap(),
    };
    let subscription = EpochChange::Subscription {
        owner: bound.id,
        expected: bound.tag.subscription,
        next: bound.tag.subscription.checked_next().unwrap(),
    };
    let book = EpochChange::Book {
        owner: bound.book_id.unwrap(),
        expected: bound.tag.book.unwrap(),
        next: bound.tag.book.unwrap().checked_next().unwrap(),
    };
    let held_close = leased(owner.reclaim_close(&mut turn, close.clone()));
    let before = handle.authority().ownership_report();
    let prefix = handle.prefix();
    let bytes = fs::read(&wal.0).unwrap();
    assert_eq!(
        sink.persist_owned(
            &mut turn,
            &qa_epoch(6, identity, connection.clone()),
            gate,
            &work
        ),
        Err(PersistBoundaryError::Authority(
            AuthorityError::NotQuiescent
        ))
    );
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
    drop(held_close);
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    let connection_record = qa_epoch(6, identity, connection.clone());
    for variant in 0..6 {
        let mut wrong = connection_record.clone();
        let Record::Control(control) = &mut wrong.value else {
            unreachable!()
        };
        match variant {
            0 => control.context.unix_ns = LocalUnixNs::new(6),
            1 => {
                control.value = Control::EpochAdvance {
                    change: subscription.clone(),
                    reason: Reason::Reconnect,
                }
            }
            2 => {
                control.value = Control::EpochAdvance {
                    change: book.clone(),
                    reason: Reason::Reconnect,
                }
            }
            3 => {
                control.value = Control::EpochAdvance {
                    change: EpochChange::Connection {
                        owner: positive(ConnectionId::new(2)),
                        expected: bound.tag.connection,
                        next: positive(ConnectionEpoch::new(2)),
                    },
                    reason: Reason::Reconnect,
                }
            }
            4 => {
                control.value = Control::EpochAdvance {
                    change: EpochChange::Connection {
                        owner: bound.connection_id,
                        expected: bound.tag.connection,
                        next: positive(ConnectionEpoch::new(3)),
                    },
                    reason: Reason::Reconnect,
                }
            }
            5 => {
                control.value = Control::EpochAdvance {
                    change: connection.clone(),
                    reason: Reason::Unknown,
                }
            }
            _ => unreachable!(),
        }
        qa_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &work,
            gate,
            &wrong,
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::NotQuiescent)
        );
    }
    sink.persist_owned(&mut turn, &connection_record, gate, &work)
        .unwrap();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &qa_epoch(7, identity, connection),
    );
    let subscription_record = qa_epoch(7, identity, subscription.clone());
    sink.persist_owned(&mut turn, &subscription_record, gate, &work)
        .unwrap();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &qa_epoch(8, identity, subscription),
    );
    let wrong_book = qa_epoch(
        8,
        identity,
        EpochChange::Book {
            owner: positive(BookId::new(2)),
            expected: bound.tag.book.unwrap(),
            next: positive(BookEpoch::new(2)),
        },
    );
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &wrong_book,
    );
    let book_record = qa_epoch(8, identity, book);
    sink.persist_owned(&mut turn, &book_record, gate, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    handle
        .authority()
        .advance_epoch(
            &mut turn,
            identity.stream,
            identity.epoch,
            positive(ConnectionEpoch::new(2)),
        )
        .unwrap();
    assert_eq!(
        &read_all(&wal.0).0[4..],
        &[down, connection_record, subscription_record, book_record]
    );
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    drop(alias);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::AlreadySettled
    ));
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::Ready(_)
    ));
}

#[test]
fn qa_new_written_generated_completion_needs_original_ordered_fresh_epoch_stages() {
    qa_generated_original_stages(RecordingGate::Written);
}

#[test]
fn qa_new_durable_generated_completion_needs_original_ordered_fresh_epoch_stages() {
    qa_generated_original_stages(RecordingGate::Durable);
}

// Integrator static-review probe for d85fa687; unexecuted when supplied.
// Either safe implementation may install Close at completion or reject while
// Pending until the rightful caller installs it. Ok without Close is forbidden.
fn qa_integrator_down_completion_keeps_original_close(gate: RecordingGate) {
    let wal = TempWal::new("qa-integrator-down-close");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let work = qa_admit(&handle, &mut turn, identity);
    let work_id = work.id();
    let original = qa_transport(5, identity, Transport::Down);
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    let prefix = handle.prefix();
    let watermarks = owner.watermarks();
    let physical = fs::read(&wal.0).unwrap();

    // No caller mandatory_close has been performed at this boundary.
    match handle.complete_observation(&mut turn, &sink, &work, None) {
        Ok(()) => {}
        Err(AuthorityError::NotQuiescent) => {
            assert_eq!(handle.authority().ownership_report().work_used, 1);
            assert_eq!(
                handle.authority().ownership_report().pending_observations,
                1
            );
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.watermarks(), watermarks);
            assert_eq!(fs::read(&wal.0).unwrap(), physical);
            handle
                .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&work))
                .unwrap();
            handle
                .complete_observation(&mut turn, &sink, &work, None)
                .unwrap();
        }
        other => panic!("unexpected original Down completion: {other:?}"),
    }
    let snapshot = owner.outstanding_close_owners();
    assert_eq!(
        snapshot.iter().count(),
        1,
        "exact received Down must not settle while its mandatory Close is absent"
    );
    let view = snapshot.iter().next().unwrap();
    let close = view.owner.clone();
    assert_eq!(view.state, CloseState::Pending);
    assert_eq!(close.stream(), identity.stream);
    assert_eq!(close.connection(), binding().connection_id);
    assert_eq!(close.epoch(), identity.epoch);
    assert_eq!(close.storage(), CloseStorage::WorkOwner(work_id));
    let ledger = handle.authority().ownership_report();
    assert_eq!(ledger.work_used, 1);
    assert_eq!(ledger.pending_observations, 0);
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::InvalidOwner)
    );
    assert_eq!(handle.authority().ownership_report(), ledger);
    let same_close = handle
        .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&work))
        .unwrap();
    assert_eq!(same_close, close);
    assert_eq!(owner.outstanding_close_owners(), snapshot);
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);

    // Release caller stewardship before Closing; the Close itself retains W.
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert_eq!(lease.work_owner_id(), Some(work_id));
    assert!(matches!(
        owner.reclaim_close(&mut turn, close.clone()),
        CloseLeaseReport::AlreadyLeased
    ));
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(lease);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    assert_eq!(read_all(&wal.0).0.len(), 5);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);

    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    let mut dispatched = 0;
    assert!(matches!(
        owner.dispatch(&mut turn, lease.into_command().unwrap(), |command| {
            assert_eq!(command.stream, identity.stream);
            assert_eq!(command.connection, binding().connection_id);
            assert_eq!(command.epoch, identity.epoch);
            assert_eq!(command.kind, &CommandKind::Close);
            dispatched += 1;
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(dispatched, 1);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::AlreadySettled
    ));
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::Ready(_)
    ));
    assert_eq!(read_all(&wal.0).0.last(), Some(&original));
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
}

#[test]
fn independent_qa_durable_exact_down_cannot_settle_without_original_mandatory_close() {
    qa_integrator_down_completion_keeps_original_close(RecordingGate::Durable);
}

#[test]
fn independent_qa_written_exact_down_cannot_settle_without_original_mandatory_close() {
    qa_integrator_down_completion_keeps_original_close(RecordingGate::Written);
}

fn qa_pending_down_close(
    owner: &CaptureSessionOwner,
    identity: ObservationIdentity,
    work: &WorkOwner,
) -> CloseOwnerRef {
    let snapshot = owner.outstanding_close_owners();
    assert_eq!(snapshot.iter().count(), 1);
    let view = snapshot.iter().next().unwrap();
    assert_eq!(view.state, CloseState::Pending);
    assert_eq!(view.owner.stream(), identity.stream);
    assert_eq!(view.owner.connection(), binding().connection_id);
    assert_eq!(view.owner.epoch(), identity.epoch);
    assert_eq!(view.owner.storage(), CloseStorage::WorkOwner(work.id()));
    view.owner.clone()
}

fn qa_down_close_foreign_and_dispatch_lifecycle(gate: RecordingGate) {
    let wal = TempWal::new("qa-down-close-lifecycle");
    let foreign_wal = TempWal::new("qa-down-close-foreign");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let (mut foreign_owner, mut foreign_turn, foreign_handle, foreign_sink) =
        owner_with_fault(&foreign_wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let work = qa_admit(&handle, &mut turn, identity);
    let work_id = work.id();
    let foreign_work = qa_admit(&foreign_handle, &mut foreign_turn, identity);
    let original = qa_transport(5, identity, Transport::Down);
    let unconfirmed = handle.authority().ownership_report();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &work, None),
        Err(AuthorityError::NotQuiescent)
    );
    assert_eq!(handle.authority().ownership_report(), unconfirmed);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    let pending = handle.authority().ownership_report();
    let status = owner.session_status();
    let prefix = handle.prefix();
    let watermarks = owner.watermarks();
    let physical = fs::read(&wal.0).unwrap();
    for variant in 0..4 {
        let result = match variant {
            0 => handle.complete_observation(&mut foreign_turn, &sink, &work, None),
            1 => handle.complete_observation(&mut turn, &foreign_sink, &work, None),
            2 => handle.complete_observation(&mut turn, &sink, &foreign_work, None),
            3 => foreign_handle.complete_observation(&mut foreign_turn, &sink, &work, None),
            _ => unreachable!(),
        };
        assert_eq!(result, Err(AuthorityError::AuthorityMismatch));
        assert_eq!(handle.authority().ownership_report(), pending);
        assert_eq!(owner.session_status(), status);
        assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
        assert_eq!(handle.prefix(), prefix);
        assert_eq!(owner.watermarks(), watermarks);
        assert_eq!(fs::read(&wal.0).unwrap(), physical);
    }
    qa_settle_once(&handle, &mut turn, &sink, &work);
    let close = qa_pending_down_close(&owner, identity, &work);
    let settled = handle.authority().ownership_report();
    assert_eq!(settled.work_used, 1);
    assert_eq!(settled.pending_observations, 0);
    assert_eq!(settled.work_references, pending.work_references + 1);
    assert_eq!(
        settled.metadata_backing_bytes,
        pending.metadata_backing_bytes
    );
    assert_eq!(
        handle
            .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&work))
            .unwrap(),
        close
    );
    assert_eq!(handle.authority().ownership_report(), settled);
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    assert_eq!(handle.authority().ownership_report().abandoned_work, 0);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let retained = handle.authority().ownership_report();
    for _ in 0..3 {
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        assert_eq!(handle.authority().ownership_report(), retained);
    }
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert_eq!(lease.work_owner_id(), Some(work_id));
    let leased_ledger = handle.authority().ownership_report();
    assert!(matches!(
        owner.reclaim_close(&mut turn, close.clone()),
        CloseLeaseReport::AlreadyLeased
    ));
    assert!(matches!(
        owner.reclaim_close(&mut foreign_turn, close.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        foreign_owner.reclaim_close(&mut foreign_turn, close.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert_eq!(handle.authority().ownership_report(), leased_ledger);
    let command = match owner.dispatch(
        &mut foreign_turn,
        lease.into_command().unwrap(),
        |_| -> Result<(), ()> { panic!("foreign turn cannot invoke the Close effect") },
    ) {
        DispatchReport::Denied {
            reason: AuthorityError::AuthorityMismatch,
            command,
        } => command,
        other => panic!("foreign turn must return the same command: {other:?}"),
    };
    let command = match foreign_owner.dispatch(&mut foreign_turn, command, |_| -> Result<(), ()> {
        panic!("foreign owner cannot invoke the Close effect")
    }) {
        DispatchReport::Denied {
            reason: AuthorityError::AuthorityMismatch,
            command,
        } => command,
        other => panic!("foreign owner must return the same command: {other:?}"),
    };
    assert_eq!(command.close_owner(), Some(&close));
    assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
    assert_eq!(handle.authority().ownership_report(), leased_ledger);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(command);
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert_eq!(handle.authority().ownership_report(), retained);
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(&mut turn, lease.into_command().unwrap(), |_| Err::<(), _>(
            "ambiguous original Close"
        )),
        DispatchReport::DispatchFailed {
            error: "ambiguous original Close",
            effect: AmbiguousEffect::Unknown
        }
    ));
    assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
    assert_eq!(handle.authority().ownership_report(), retained);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    let mut dispatched = 0;
    assert!(matches!(
        owner.dispatch(&mut turn, lease.into_command().unwrap(), |command| {
            assert_eq!(command.stream, identity.stream);
            assert_eq!(command.connection, binding().connection_id);
            assert_eq!(command.epoch, identity.epoch);
            assert_eq!(command.kind, &CommandKind::Close);
            dispatched += 1;
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(dispatched, 1);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::AlreadySettled
    ));
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("rightful Close enables the original sole proof")
    };
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    if gate == RecordingGate::Durable {
        owner.finalize(&mut turn, &mut proof).unwrap();
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::Complete);
        assert_eq!(report.input_quality, Some(InputQuality::Unknown));
        assert_eq!(records.len(), 7);
        assert_eq!(records[4], original);
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.value, Record::SegmentSeal(_)))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.value, Record::ArchiveSeal(_)))
                .count(),
            1
        );
        let finalized = fs::read(&wal.0).unwrap();
        assert!(owner.finalize(&mut turn, &mut proof).is_err());
        assert_eq!(fs::read(&wal.0).unwrap(), finalized);
    } else {
        assert_eq!(read_all(&wal.0).0.len(), 5);
        assert_eq!(fs::read(&wal.0).unwrap(), physical);
    }
}

#[test]
fn qa_down_written_public_completion_retains_close_through_foreign_drop_error_and_recovery() {
    qa_down_close_foreign_and_dispatch_lifecycle(RecordingGate::Written);
}

#[test]
fn qa_down_durable_public_completion_retains_close_through_foreign_drop_error_and_recovery() {
    qa_down_close_foreign_and_dispatch_lifecycle(RecordingGate::Durable);
}

fn qa_down_close_alias_exhaustion(gate: RecordingGate) {
    let wal = TempWal::new("qa-down-close-share-limit");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let work = qa_admit(&handle, &mut turn, identity);
    let original = qa_transport(5, identity, Transport::Down);
    sink.persist_owned(&mut turn, &original, gate, &work)
        .unwrap();
    let mut aliases = Vec::new();
    for _ in 0..8 {
        match work.share() {
            Ok(alias) => aliases.push(alias),
            Err(AuthorityError::WorkShareExhausted) => break,
            other => panic!("unexpected bounded share result: {other:?}"),
        }
    }
    assert!(!aliases.is_empty());
    assert!(matches!(
        work.share(),
        Err(AuthorityError::WorkShareExhausted)
    ));
    let pending = handle.authority().ownership_report();
    let prefix = handle.prefix();
    let watermarks = owner.watermarks();
    let physical = fs::read(&wal.0).unwrap();
    for _ in 0..3 {
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &work, None),
            Err(AuthorityError::WorkShareExhausted)
        );
        assert_eq!(handle.authority().ownership_report(), pending);
        assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
        assert_eq!(handle.prefix(), prefix);
        assert_eq!(owner.watermarks(), watermarks);
        assert_eq!(fs::read(&wal.0).unwrap(), physical);
    }
    assert_eq!(pending.work_used, 1);
    assert_eq!(pending.pending_observations, 1);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(aliases.pop().unwrap());
    qa_settle_once(&handle, &mut turn, &sink, &work);
    let close = qa_pending_down_close(&owner, identity, &work);
    drop(aliases);
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    assert_eq!(handle.authority().ownership_report().abandoned_work, 0);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    let lease = leased(owner.reclaim_close(&mut turn, close));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::Ready(_)
    ));
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
}

#[test]
fn qa_down_written_close_install_share_limit_preserves_pending_and_rightful_retry() {
    qa_down_close_alias_exhaustion(RecordingGate::Written);
}

#[test]
fn qa_down_durable_close_install_share_limit_preserves_pending_and_rightful_retry() {
    qa_down_close_alias_exhaustion(RecordingGate::Durable);
}

fn qa_down_close_duplicate_prior_and_terminal_reuse(gate: RecordingGate) {
    for variant in 0..4 {
        let wal = TempWal::new("qa-down-close-prior-reuse");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let first_identity = qa_original_identity(ObservationClass::Disconnected);
        let first = qa_admit(&handle, &mut turn, first_identity);
        let mut next_identity = first_identity;
        next_identity.stamp = ReceiveStamp {
            unix_ns: 6,
            monotonic_ns: 6,
        };
        let next = qa_admit(&handle, &mut turn, next_identity);
        sink.persist_owned(
            &mut turn,
            &qa_transport(5, first_identity, Transport::Down),
            gate,
            &first,
        )
        .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &first);
        let close = qa_pending_down_close(&owner, first_identity, &first);
        let mut held = None;
        if variant != 0 {
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            if variant == 2 {
                assert!(matches!(
                    owner.dispatch(
                        &mut turn,
                        lease.into_command().unwrap(),
                        |_| Ok::<_, ()>(())
                    ),
                    DispatchReport::Dispatched
                ));
            } else {
                held = Some(lease);
            }
        }
        if variant == 3 {
            let termination = handle.terminate(&mut turn, terminal()).unwrap();
            assert_eq!(termination.close_owner, close);
            assert!(termination.close.is_none());
        }
        let state = if variant == 2 {
            CloseState::Settled
        } else if held.is_some() {
            CloseState::Leased
        } else {
            CloseState::Pending
        };
        let original_cut = owner.session_status().cut_sequence;
        let original_close = owner.outstanding_close_owners();
        sink.persist_owned(
            &mut turn,
            &qa_transport(6, next_identity, Transport::Down),
            gate,
            &next,
        )
        .unwrap();
        let before = handle.authority().ownership_report();
        qa_settle_once(&handle, &mut turn, &sink, &next);
        let after = handle.authority().ownership_report();
        let mut expected = before;
        expected.pending_observations -= 1;
        assert_eq!(
            after, expected,
            "duplicate Down settles only its original observation; no second owner/alias"
        );
        assert_eq!(owner.outstanding_close_owners(), original_close);
        assert_eq!(owner.session_status().cut_sequence, original_cut);
        assert_eq!(
            handle
                .authority()
                .close_state(first_identity.stream, first_identity.epoch),
            Ok(state)
        );
        assert_eq!(
            handle
                .mandatory_close(
                    &mut turn,
                    next_identity.stream,
                    next_identity.epoch,
                    Some(&next)
                )
                .unwrap(),
            close
        );
        assert_eq!(handle.authority().ownership_report(), after);
        assert_eq!(close.storage(), CloseStorage::WorkOwner(first.id()));
        drop(first);
        drop(next);
        assert_eq!(
            handle.authority().ownership_report().work_used,
            usize::from(state != CloseState::Settled)
        );
        drop(held);
        if state != CloseState::Settled {
            assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
        }
        assert!(matches!(
            owner.reclaim_close(&mut turn, close),
            CloseLeaseReport::AlreadySettled
        ));
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        if variant == 3 {
            assert!(matches!(
                owner.begin_finalization(&mut turn),
                Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
            ));
        } else {
            let ticket = owner.begin_finalization(&mut turn).unwrap();
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::Ready(_)
            ));
        }
        let (records, report) = read_all(&wal.0);
        assert_eq!(records.len(), 6);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(!records.iter().any(|record| matches!(
            record.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        )));
    }
}

#[test]
fn qa_down_written_duplicate_prior_and_terminal_close_reuse_preserves_one_owner() {
    qa_down_close_duplicate_prior_and_terminal_reuse(RecordingGate::Written);
}

#[test]
fn qa_down_durable_duplicate_prior_and_terminal_close_reuse_preserves_one_owner() {
    qa_down_close_duplicate_prior_and_terminal_reuse(RecordingGate::Durable);
}

fn qa_down_close_rejects_unrelated_live_owner(gate: RecordingGate) {
    for variant in 0..4 {
        if variant == 3 {
            // Approved Timer A closes the former generic Timer-only mint route.
            // Check its preserving rejection and a genuine Down/Close control.
            qa_timer_ping_cannot_mint_close(gate);
            continue;
        }
        let wal = TempWal::new("qa-down-close-unrelated-owner");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let other_identity = qa_original_identity(ObservationClass::Raw);
        let other = if variant == 0 {
            handle
                .reserve_work(&mut turn, WorkKind::PendingPlan)
                .unwrap()
        } else {
            qa_admit(&handle, &mut turn, other_identity)
        };
        let mut down_no = 5;
        if variant == 2 {
            let other_record = qa_raw(5, other_identity, false);
            sink.persist_owned(&mut turn, &other_record, gate, &other)
                .unwrap();
            qa_settle_once(&handle, &mut turn, &sink, &other);
            down_no = 6;
        }
        let close = handle
            .mandatory_close(
                &mut turn,
                binding().id,
                binding().tag.connection,
                Some(&other),
            )
            .unwrap();
        let held = leased(owner.reclaim_close(&mut turn, close.clone()));
        let identity = qa_original_identity(ObservationClass::Disconnected);
        let down = qa_admit(&handle, &mut turn, identity);
        if variant == 1 {
            qa_timer_reject(
                (&wal, &owner, &handle),
                &mut turn,
                &mut sink,
                &down,
                gate,
                &qa_transport(5, identity, Transport::Down),
                AuthorityError::TimerOrderBlocked {
                    earlier_work_id: other.id(),
                },
            );
            // The earlier original Raw receipt removes the FIFO barrier while
            // its completion remains Pending. Its unrelated live Close still
            // cannot account for the later received Down's completion.
            sink.persist_owned(&mut turn, &qa_raw(5, other_identity, false), gate, &other)
                .unwrap();
            down_no = 6;
        }
        let original = qa_transport(down_no, identity, Transport::Down);
        sink.persist_owned(&mut turn, &original, gate, &down)
            .unwrap();
        let before = handle.authority().ownership_report();
        let status = owner.session_status();
        let closes = owner.outstanding_close_owners();
        let prefix = handle.prefix();
        let watermarks = owner.watermarks();
        let bytes = fs::read(&wal.0).unwrap();
        for _ in 0..3 {
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, &down, None),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(handle.authority().ownership_report(), before);
            assert_eq!(owner.session_status(), status);
            assert_eq!(owner.outstanding_close_owners(), closes);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.watermarks(), watermarks);
            assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        }
        assert_eq!(close.storage(), CloseStorage::WorkOwner(other.id()));
        assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        assert!(matches!(
            owner.dispatch(&mut turn, held.into_command().unwrap(), |_| Ok::<_, ()>(())),
            DispatchReport::Dispatched
        ));
        // The earlier Close is now actually fulfilled; retry reuses its settled
        // identity without installing a replacement owner on the received Down.
        qa_settle_once(&handle, &mut turn, &sink, &down);
        assert_eq!(
            handle
                .authority()
                .close_state(identity.stream, identity.epoch),
            Ok(CloseState::Settled)
        );
        assert_eq!(
            handle
                .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&down))
                .unwrap(),
            close
        );
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        if variant == 1 {
            qa_settle_once(&handle, &mut turn, &sink, &other);
        }
        drop(other);
        drop(down);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::Ready(_)
        ));
        let records = read_all(&wal.0).0;
        assert_eq!(records.len(), down_no as usize);
        if variant == 1 {
            assert_eq!(&records[4..], &[qa_raw(5, other_identity, false), original]);
        }
        assert!(!records.iter().any(|record| matches!(
            record.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        )));
    }
}

#[test]
fn qa_down_written_incompatible_live_close_preserves_pending_until_truthful_settlement() {
    qa_down_close_rejects_unrelated_live_owner(RecordingGate::Written);
}

#[test]
fn qa_down_durable_incompatible_live_close_preserves_pending_until_truthful_settlement() {
    qa_down_close_rejects_unrelated_live_owner(RecordingGate::Durable);
}

fn qa_down_close_failure_cut_neighbor_and_abandonment(gate: RecordingGate) {
    for mismatch in [false, true] {
        let wal = TempWal::new("qa-down-close-failure-cut");
        let (mut owner, mut turn, handle, mut sink) = owner_two_scopes_with_gate(&wal.0, gate);
        let identity = qa_original_identity(ObservationClass::Disconnected);
        let down = qa_admit(&handle, &mut turn, identity);
        let mut neighbor_identity = qa_original_identity(ObservationClass::Raw);
        neighbor_identity.stream = positive(StreamId::new(2));
        let neighbor = qa_admit(&handle, &mut turn, neighbor_identity);
        sink.persist_owned(
            &mut turn,
            &qa_transport(7, identity, Transport::Down),
            gate,
            &down,
        )
        .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &down);
        let close = qa_pending_down_close(&owner, identity, &down);
        let held = leased(owner.reclaim_close(&mut turn, close.clone()));
        let failure = terminal();
        let termination = handle.terminate(&mut turn, failure).unwrap();
        assert_eq!(termination.close_owner, close);
        assert!(termination.close.is_none());
        let cut = owner.session_status().cut_sequence;
        assert_eq!(down.cut_side(), domain::capture_session::CutSide::PreCut);
        assert_eq!(
            neighbor.cut_side(),
            domain::capture_session::CutSide::PreCut
        );
        let neighbor_raw = qa_raw(8, neighbor_identity, false);
        sink.persist_owned(&mut turn, &neighbor_raw, gate, &neighbor)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &neighbor);
        drop(neighbor);
        assert_eq!(owner.session_status().cut_sequence, cut);
        assert_eq!(owner.session_status().first_failure, Some(failure));
        assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
        let before = handle.authority().ownership_report();
        let prefix = handle.prefix();
        let error = PersistError::typed(PersistErrorKind::Io, "received Down marker error");
        let kind = if mismatch {
            SinkFaultKind::ReceiptMismatch { through: record(8) }
        } else {
            SinkFaultKind::BeforeWrite(error)
        };
        owner
            .set_sink_fault(
                &mut turn,
                Some(SinkFault {
                    at: record(9),
                    kind,
                }),
            )
            .unwrap();
        let mut marker = failed_marker();
        marker.record_no = record(9);
        let Record::Control(ControlRecord {
            value: Control::Recording(evidence),
            ..
        }) = &mut marker.value
        else {
            unreachable!()
        };
        evidence.kind = if gate == RecordingGate::Durable {
            WatermarkKind::Durable
        } else {
            WatermarkKind::Written
        };
        evidence.through = Some(record(8));
        let result = sink.persist(&mut turn, &marker, gate);
        if mismatch {
            assert_eq!(
                result,
                Err(PersistBoundaryError::ReceiptMismatch {
                    expected: record(9),
                    actual: record(8)
                })
            );
        } else {
            assert_eq!(result, Err(PersistBoundaryError::Persistence(error)));
        }
        assert_eq!(handle.authority().ownership_report(), before);
        assert_eq!(handle.prefix(), prefix);
        assert!(owner.session_status().storage_stopped.is_some());
        assert_eq!(owner.session_status().cut_sequence, cut);
        assert_eq!(owner.session_status().first_failure, Some(failure));
        assert_eq!(close_state(&owner, &close), Some(CloseState::Leased));
        drop(down);
        assert_eq!(handle.authority().ownership_report().work_used, 1);
        drop(held);
        assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        assert!(matches!(
            owner.reclaim_close(&mut turn, close),
            CloseLeaseReport::AlreadySettled
        ));
        assert!(matches!(
            owner.begin_finalization(&mut turn),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        let (records, report) = read_all(&wal.0);
        assert_eq!(records[7], neighbor_raw);
        assert_eq!(records.len(), 8 + usize::from(mismatch));
        if mismatch {
            assert_eq!(records.last(), Some(&marker));
        }
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(!records.iter().any(|record| matches!(
            record.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        )));
    }
    let wal = TempWal::new("qa-down-close-abandoned-receipt");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let down = qa_admit(&handle, &mut turn, identity);
    let work_id = down.id();
    sink.persist_owned(
        &mut turn,
        &qa_transport(5, identity, Transport::Down),
        gate,
        &down,
    )
    .unwrap();
    let prefix = handle.prefix();
    let bytes = fs::read(&wal.0).unwrap();
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(down);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
    ));
    let abandoned = owner.session_status().first_abandonment.unwrap();
    assert_eq!(abandoned.identity, identity);
    assert_eq!(abandoned.work_id, work_id);
    assert_eq!(
        owner.session_status().storage_stopped.unwrap().kind,
        PersistErrorKind::OwnershipAbandoned
    );
    assert_eq!(handle.authority().ownership_report().abandoned_work, 1);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    let snapshot = owner.outstanding_close_owners();
    assert_eq!(snapshot.iter().count(), 1);
    let close = snapshot.iter().next().unwrap().owner.clone();
    assert_eq!(close.storage(), CloseStorage::ReservedTerminal);
    let lease = leased(owner.reclaim_close(&mut turn, close));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
    ));
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    let records = read_all(&wal.0).0;
    assert_eq!(records.len(), 5);
    assert!(!records.iter().any(|record| matches!(
        record.value,
        Record::SegmentSeal(_) | Record::ArchiveSeal(_)
    )));
}

#[test]
fn qa_down_written_failure_cut_neighbor_and_abandonment_preserve_truthful_close() {
    qa_down_close_failure_cut_neighbor_and_abandonment(RecordingGate::Written);
}

#[test]
fn qa_down_durable_failure_cut_neighbor_and_abandonment_preserve_truthful_close() {
    qa_down_close_failure_cut_neighbor_and_abandonment(RecordingGate::Durable);
}

fn qa_down_close_reserved_terminal_reuse(gate: RecordingGate) {
    for variant in 0..3 {
        let wal = TempWal::new("qa-down-close-reserved-terminal");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let identity = qa_original_identity(ObservationClass::Disconnected);
        let down = qa_admit(&handle, &mut turn, identity);
        let failure = terminal();
        let termination = handle.terminate(&mut turn, failure).unwrap();
        let close = termination.close_owner;
        assert_eq!(close.storage(), CloseStorage::ReservedTerminal);
        let lease = termination
            .close
            .expect("one original reserved terminal lease");
        assert_eq!(lease.work_owner_id(), None);
        let held =
            if variant == 1 {
                Some(lease)
            } else {
                if variant == 2 {
                    assert!(matches!(
                        owner.dispatch(&mut turn, lease.into_command().unwrap(), |_| Ok::<_, ()>(
                            ()
                        )),
                        DispatchReport::Dispatched
                    ));
                } else {
                    drop(lease);
                }
                None
            };
        let expected_state = match variant {
            0 => CloseState::Pending,
            1 => CloseState::Leased,
            2 => CloseState::Settled,
            _ => unreachable!(),
        };
        let cut = owner.session_status().cut_sequence;
        let original = qa_transport(5, identity, Transport::Down);
        sink.persist_owned(&mut turn, &original, gate, &down)
            .unwrap();
        let before = handle.authority().ownership_report();
        let closes = owner.outstanding_close_owners();
        let prefix = handle.prefix();
        let watermarks = owner.watermarks();
        let bytes = fs::read(&wal.0).unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &down);
        let mut expected = before;
        expected.pending_observations -= 1;
        assert_eq!(handle.authority().ownership_report(), expected);
        assert_eq!(owner.outstanding_close_owners(), closes);
        assert_eq!(
            handle
                .authority()
                .close_state(identity.stream, identity.epoch),
            Ok(expected_state)
        );
        assert_eq!(
            handle
                .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&down))
                .unwrap(),
            close
        );
        assert_eq!(handle.authority().ownership_report(), expected);
        assert_eq!(owner.session_status().first_failure, Some(failure));
        assert_eq!(owner.session_status().cut_sequence, cut);
        assert_eq!(handle.prefix(), prefix);
        assert_eq!(owner.watermarks(), watermarks);
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        drop(down);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        drop(held);
        if expected_state != CloseState::Settled {
            assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            assert_eq!(lease.work_owner_id(), None);
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::AlreadyLeased
            ));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
        }
        assert!(matches!(
            owner.reclaim_close(&mut turn, close),
            CloseLeaseReport::AlreadySettled
        ));
        assert!(matches!(
            owner.begin_finalization(&mut turn),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        assert_eq!(owner.session_status().cut_sequence, cut);
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        let (records, report) = read_all(&wal.0);
        assert_eq!(records.len(), 5);
        assert_eq!(records.last(), Some(&original));
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(!records.iter().any(|record| matches!(
            record.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        )));
    }
}

#[test]
fn qa_down_written_preexisting_reserved_terminal_close_is_reused_in_all_states() {
    qa_down_close_reserved_terminal_reuse(RecordingGate::Written);
}

#[test]
fn qa_down_durable_preexisting_reserved_terminal_close_is_reused_in_all_states() {
    qa_down_close_reserved_terminal_reuse(RecordingGate::Durable);
}

// ARCH-REC-001D-TIMER-A-20261008: public owner/bound-sink integration probes.
// Durable entries use the real file backend and original authority capabilities.
// Written entries are supplemental controls.
const QA_PING_NS: u64 = 30_000_000_000;
const QA_PONG_NS: u64 = 15_000_000_000;

fn qa_timer_stamp(monotonic_ns: u64) -> ReceiveStamp {
    ReceiveStamp {
        unix_ns: 17,
        monotonic_ns,
    }
}

fn qa_timer_frame(number: u64, identity: ObservationIdentity) -> RecordFrame {
    let ObservationClass::Timer {
        timer_id,
        deadline_ns,
    } = identity.class
    else {
        panic!("original authority Timer required");
    };
    frame(
        number,
        Record::Control(ControlRecord {
            context: qa_context(identity),
            value: Control::Timer {
                stream: identity.stream,
                timer_id,
                deadline_ns,
            },
        }),
    )
}

fn qa_timer_transport(number: u64, identity: ObservationIdentity, value: Transport) -> RecordFrame {
    let mut original = qa_transport(number, identity, value);
    let Record::Control(ControlRecord {
        value: Control::Transport { connection, .. },
        ..
    }) = &mut original.value
    else {
        unreachable!();
    };
    *connection = positive(ConnectionId::new(identity.stream.get()));
    original
}

fn qa_timer_install_up(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
    stream: StreamId,
    stamp: u64,
) {
    let mut identity = qa_original_identity(ObservationClass::Connected);
    identity.stream = stream;
    identity.stamp = qa_timer_stamp(stamp);
    let work = qa_admit(handle, turn, identity);
    sink.persist_owned(
        turn,
        &qa_timer_transport(handle.prefix().next_record.get(), identity, Transport::Up),
        gate,
        &work,
    )
    .unwrap();
    qa_settle_once(handle, turn, sink, &work);
    drop(work);
}

fn qa_timer_due(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    stream: StreamId,
    stamp: u64,
) -> AdmittedTimer {
    let TimerAdmission::Admitted(timer) = handle
        .admit_due_timer(turn, stream, qa_timer_stamp(stamp))
        .unwrap()
    else {
        panic!("one genuine original due Timer required");
    };
    timer
        .owner()
        .set_kind(turn, WorkKind::InFlightObservation)
        .unwrap();
    timer
}

fn qa_timer_arm_timeout(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
    stream: StreamId,
) {
    qa_timer_install_up(handle, turn, sink, gate, stream, 5);
    let ping = qa_timer_due(handle, turn, stream, 5 + QA_PING_NS);
    assert_eq!(ping.kind(), TimerKind::Ping);
    sink.persist_owned(
        turn,
        &qa_timer_frame(handle.prefix().next_record.get(), ping.identity()),
        gate,
        ping.owner(),
    )
    .unwrap();
    let lease = handle.take_timer_ping(turn, ping.owner()).unwrap();
    qa_settle_once(handle, turn, sink, ping.owner());
    assert!(matches!(
        handle
            .authority()
            .dispatch(turn, lease, |_| Ok::<_, ()>(())),
        DispatchReport::Dispatched
    ));
    drop(ping);
}

fn qa_timer_timeout(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
    stream: StreamId,
) -> AdmittedTimer {
    qa_timer_arm_timeout(handle, turn, sink, gate, stream);
    let timeout = qa_timer_due(handle, turn, stream, 5 + QA_PING_NS + QA_PONG_NS);
    assert_eq!(timeout.kind(), TimerKind::Timeout);
    timeout
}

fn qa_timer_reject(
    boundary: (&TempWal, &CaptureSessionOwner, &SupervisorSessionHandle),
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    work: &WorkOwner,
    gate: RecordingGate,
    submitted: &RecordFrame,
    expected: AuthorityError,
) {
    let (wal, owner, handle) = boundary;
    let ledger = handle.authority().ownership_report();
    let status = owner.session_status();
    let prefix = handle.prefix();
    let closes = owner.outstanding_close_owners();
    let watermarks = owner.watermarks();
    let progress = handle.timer_progress(turn, work);
    let physical = fs::read(&wal.0).unwrap();
    let inventory = read_all(&wal.0).0;
    let backend_calls = owner.sink_persist_calls();
    assert_eq!(
        sink.persist_owned(turn, submitted, gate, work),
        Err(PersistBoundaryError::Authority(expected))
    );
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(owner.session_status(), status);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.outstanding_close_owners(), closes);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(handle.timer_progress(turn, work), progress);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
    assert_eq!(read_all(&wal.0).0, inventory);
    assert_eq!(owner.sink_persist_calls(), backend_calls);
}

fn qa_timer_selected_close(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    work: &WorkOwner,
) -> CloseOwnerRef {
    let TimerProgressView::TimerThenDown {
        timer_confirmed,
        down_confirmed,
        close,
    } = handle.timer_progress(turn, work).unwrap()
    else {
        panic!("private original Timeout plan expected");
    };
    assert!(timer_confirmed);
    assert!(!down_confirmed);
    assert!(!close.ready);
    assert_eq!(close.state, CloseState::Pending);
    assert_eq!(close.owner.storage(), CloseStorage::WorkOwner(work.id()));
    close.owner
}

fn qa_timer_commit_down(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
    timer: &AdmittedTimer,
) -> RecordFrame {
    let down = qa_timer_transport(
        handle.prefix().next_record.get(),
        timer.identity(),
        Transport::Down,
    );
    sink.persist_owned(turn, &down, gate, timer.owner())
        .unwrap();
    let TimerProgressView::TimerThenDown {
        timer_confirmed,
        down_confirmed,
        close,
    } = handle.timer_progress(turn, timer.owner()).unwrap()
    else {
        panic!("original Timeout stages expected");
    };
    assert!(timer_confirmed && down_confirmed && close.ready);
    assert_eq!(
        close.owner.storage(),
        CloseStorage::WorkOwner(timer.owner().id())
    );
    down
}

fn qa_timer_ping_cannot_mint_close(gate: RecordingGate) {
    let wal = TempWal::new("timer-ping-close-mint");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    let ping = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let before = handle.authority().ownership_report();
    let physical = fs::read(&wal.0).unwrap();
    assert_eq!(
        handle.mandatory_close(
            &mut turn,
            binding().id,
            binding().tag.connection,
            Some(ping.owner())
        ),
        Err(AuthorityError::TimerAuthorityRequired)
    );
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    sink.persist_owned(
        &mut turn,
        &qa_timer_frame(6, ping.identity()),
        gate,
        ping.owner(),
    )
    .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, ping.owner());
    let command = handle.take_timer_ping(&mut turn, ping.owner()).unwrap();
    drop(command);
    drop(ping);
    let identity = qa_original_identity(ObservationClass::Disconnected);
    let down = qa_admit(&handle, &mut turn, identity);
    sink.persist_owned(
        &mut turn,
        &qa_transport(7, identity, Transport::Down),
        gate,
        &down,
    )
    .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &down);
    let close = qa_pending_down_close(&owner, identity, &down);
    assert_eq!(close.storage(), CloseStorage::WorkOwner(down.id()));
    assert_eq!(read_all(&wal.0).0.len(), 7);
}

fn qa_timer_t01_admission_order(gate: RecordingGate) {
    for class in [ObservationClass::Pong, ObservationClass::Connected] {
        for offset in [-1_i64, 0, 1] {
            for timer_first in [false, true] {
                let wal = TempWal::new("timer-t01-order");
                let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
                qa_timer_arm_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
                let deadline = 5 + QA_PING_NS + QA_PONG_NS;
                let mut identity = qa_original_identity(class);
                identity.stamp = qa_timer_stamp(deadline.checked_add_signed(offset).unwrap());
                let earlier = (!timer_first).then(|| qa_admit(&handle, &mut turn, identity));
                let timer = qa_timer_due(&handle, &mut turn, binding().id, deadline);
                let received = earlier.unwrap_or_else(|| qa_admit(&handle, &mut turn, identity));
                let original_timer = qa_timer_frame(7, timer.identity());
                let original_up = qa_timer_transport(7, identity, Transport::Up);
                if timer_first {
                    qa_timer_reject(
                        (&wal, &owner, &handle),
                        &mut turn,
                        &mut sink,
                        &received,
                        gate,
                        &original_up,
                        AuthorityError::TimerOrderBlocked {
                            earlier_work_id: timer.owner().id(),
                        },
                    );
                    sink.persist_owned(&mut turn, &original_timer, gate, timer.owner())
                        .unwrap();
                    let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
                    assert!(matches!(
                        owner.reclaim_close(&mut turn, close.clone()),
                        CloseLeaseReport::Rejected(AuthorityError::CloseNotReady)
                    ));
                    assert_eq!(
                        handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                        Err(AuthorityError::NotQuiescent)
                    );
                    let down = qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
                    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
                    let before_calls = owner.sink_persist_calls();
                    let before_bytes = fs::read(&wal.0).unwrap();
                    handle
                        .complete_observation(
                            &mut turn,
                            &sink,
                            &received,
                            Some((identity.stream, identity.epoch)),
                        )
                        .unwrap();
                    assert_eq!(owner.sink_persist_calls(), before_calls);
                    assert_eq!(fs::read(&wal.0).unwrap(), before_bytes);
                    assert_eq!(&read_all(&wal.0).0[6..], &[original_timer, down]);
                } else {
                    qa_timer_reject(
                        (&wal, &owner, &handle),
                        &mut turn,
                        &mut sink,
                        timer.owner(),
                        gate,
                        &original_timer,
                        AuthorityError::TimerOrderBlocked {
                            earlier_work_id: received.id(),
                        },
                    );
                    sink.persist_owned(&mut turn, &original_up, gate, &received)
                        .unwrap();
                    qa_settle_once(&handle, &mut turn, &sink, &received);
                    let obsolete = qa_timer_frame(8, timer.identity());
                    sink.persist_owned(&mut turn, &obsolete, gate, timer.owner())
                        .unwrap();
                    assert_eq!(
                        handle.timer_progress(&mut turn, timer.owner()).unwrap(),
                        TimerProgressView::TimerOnlyObsolete {
                            timer_confirmed: true
                        }
                    );
                    assert!(matches!(
                        handle.take_timer_ping(&mut turn, timer.owner()),
                        Err(AuthorityError::CommandRevoked)
                    ));
                    qa_timer_reject(
                        (&wal, &owner, &handle),
                        &mut turn,
                        &mut sink,
                        timer.owner(),
                        gate,
                        &qa_timer_transport(9, timer.identity(), Transport::Down),
                        AuthorityError::InvalidBinding,
                    );
                    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
                    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
                    assert_eq!(&read_all(&wal.0).0[6..], &[original_up, obsolete]);
                }
                assert_eq!(
                    read_all(&wal.0).1.status,
                    ArchiveStatus::ValidPrefixIncomplete
                );
            }
        }
    }
}

#[test]
fn timer_a_t01_durable_original_admission_order_wins_pong_and_connected_deadline_ties() {
    qa_timer_t01_admission_order(RecordingGate::Durable);
}
#[test]
fn timer_a_t01_written_original_admission_order_wins_pong_and_connected_deadline_ties() {
    qa_timer_t01_admission_order(RecordingGate::Written);
}

fn qa_timer_t02_record_order(gate: RecordingGate) {
    for class in [
        ObservationClass::Raw,
        ObservationClass::Gap,
        ObservationClass::RejectedStaleRaw,
    ] {
        for raw_first in [false, true] {
            let wal = TempWal::new("timer-t02-record-order");
            let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
            qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
            // Reservation ID predates Timer; only successful record admission
            // determines whether this obligation is actually an earlier barrier.
            let raw = handle
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap();
            let identity = qa_original_identity(class);
            if raw_first {
                handle.admit_observation(&mut turn, &raw, identity).unwrap();
            }
            let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
            assert!(raw.id() < timer.owner().id());
            if !raw_first {
                handle.admit_observation(&mut turn, &raw, identity).unwrap();
            }
            raw.set_kind(&mut turn, WorkKind::InFlightObservation)
                .unwrap();
            let timer_record = qa_timer_frame(6, timer.identity());
            if raw_first {
                qa_timer_reject(
                    (&wal, &owner, &handle),
                    &mut turn,
                    &mut sink,
                    timer.owner(),
                    gate,
                    &timer_record,
                    AuthorityError::TimerOrderBlocked {
                        earlier_work_id: raw.id(),
                    },
                );
                let first = if class == ObservationClass::Gap {
                    qa_gap(6, identity, Reason::QueueOverflow)
                } else {
                    qa_raw(6, identity, class == ObservationClass::RejectedStaleRaw)
                };
                sink.persist_owned(&mut turn, &first, gate, &raw).unwrap();
                if class == ObservationClass::RejectedStaleRaw {
                    qa_timer_reject(
                        (&wal, &owner, &handle),
                        &mut turn,
                        &mut sink,
                        timer.owner(),
                        gate,
                        &qa_timer_frame(7, timer.identity()),
                        AuthorityError::TimerOrderBlocked {
                            earlier_work_id: raw.id(),
                        },
                    );
                    sink.persist_owned(
                        &mut turn,
                        &qa_gap(7, identity, Reason::Unknown),
                        gate,
                        &raw,
                    )
                    .unwrap();
                }
                // Exact receipts, with the original Raw alias still held, cease
                // to block the later Timer even before queue completion.
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_frame(handle.prefix().next_record.get(), timer.identity()),
                    gate,
                    timer.owner(),
                )
                .unwrap();
            } else {
                sink.persist_owned(&mut turn, &timer_record, gate, timer.owner())
                    .unwrap();
                let first = if class == ObservationClass::Gap {
                    qa_gap(7, identity, Reason::QueueOverflow)
                } else {
                    qa_raw(7, identity, class == ObservationClass::RejectedStaleRaw)
                };
                sink.persist_owned(&mut turn, &first, gate, &raw).unwrap();
                if class == ObservationClass::RejectedStaleRaw {
                    sink.persist_owned(
                        &mut turn,
                        &qa_gap(8, identity, Reason::Unknown),
                        gate,
                        &raw,
                    )
                    .unwrap();
                }
            }
            qa_settle_once(&handle, &mut turn, &sink, &raw);
            qa_settle_once(&handle, &mut turn, &sink, timer.owner());
            drop(handle.take_timer_ping(&mut turn, timer.owner()).unwrap());
            assert_eq!(
                owner.sink_persist_calls(),
                if class == ObservationClass::RejectedStaleRaw {
                    4
                } else {
                    3
                }
            );
            assert_eq!(
                read_all(&wal.0).1.status,
                ArchiveStatus::ValidPrefixIncomplete
            );
        }
    }
}

#[test]
fn timer_a_t02_durable_fifo_uses_admission_order_and_all_original_raw_gap_stages() {
    qa_timer_t02_record_order(RecordingGate::Durable);
}
#[test]
fn timer_a_t02_written_fifo_uses_admission_order_and_all_original_raw_gap_stages() {
    qa_timer_t02_record_order(RecordingGate::Written);
}

fn qa_timer_t03_frozen_interstage(gate: RecordingGate) {
    for class in [
        ObservationClass::Pong,
        ObservationClass::Connected,
        ObservationClass::Disconnected,
    ] {
        let wal = TempWal::new("timer-t03-frozen-plan");
        let (owner, mut turn, handle, mut sink) = owner_two_scopes_with_gate(&wal.0, gate);
        let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
        sink.persist_owned(
            &mut turn,
            &qa_timer_frame(9, timer.identity()),
            gate,
            timer.owner(),
        )
        .unwrap();
        let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
        let mut identity = qa_original_identity(class);
        identity.stamp = qa_timer_stamp(timer.identity().stamp.monotonic_ns + 1);
        let received = qa_admit(&handle, &mut turn, identity);
        let proposed = qa_timer_transport(
            10,
            identity,
            if class == ObservationClass::Disconnected {
                Transport::Down
            } else {
                Transport::Up
            },
        );
        qa_timer_reject(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &received,
            gate,
            &proposed,
            AuthorityError::TimerOrderBlocked {
                earlier_work_id: timer.owner().id(),
            },
        );
        let before = handle.authority().ownership_report();
        assert_eq!(
            handle.authority().advance_epoch(
                &mut turn,
                binding().id,
                binding().tag.connection,
                positive(ConnectionEpoch::new(2))
            ),
            Err(AuthorityError::TimerPlanInProgress {
                work_id: timer.owner().id()
            })
        );
        assert_eq!(handle.authority().ownership_report(), before);
        // A neighboring scope remains independently eligible during the freeze.
        qa_timer_install_up(
            &handle,
            &mut turn,
            &mut sink,
            gate,
            positive(StreamId::new(2)),
            7,
        );
        assert_eq!(
            qa_timer_selected_close(&handle, &mut turn, timer.owner()),
            close
        );
        let down = qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        assert_eq!(down.record_no, record(11));
        assert_eq!(
            read_all(&wal.0).1.status,
            ArchiveStatus::ValidPrefixIncomplete
        );
        if class != ObservationClass::Disconnected {
            let bytes = fs::read(&wal.0).unwrap();
            let calls = owner.sink_persist_calls();
            handle
                .complete_observation(
                    &mut turn,
                    &sink,
                    &received,
                    Some((identity.stream, identity.epoch)),
                )
                .unwrap();
            assert_eq!(fs::read(&wal.0).unwrap(), bytes);
            assert_eq!(owner.sink_persist_calls(), calls);
        }
    }
}

#[test]
fn timer_a_t03_durable_timeout_plan_freezes_same_scope_and_allows_neighbor() {
    qa_timer_t03_frozen_interstage(RecordingGate::Durable);
}
#[test]
fn timer_a_t03_written_timeout_plan_freezes_same_scope_and_allows_neighbor() {
    qa_timer_t03_frozen_interstage(RecordingGate::Written);
}

fn qa_timer_t04_closed_legacy_routes(gate: RecordingGate) {
    let wal = TempWal::new("timer-t04-legacy-closure");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let work = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    let identity = qa_original_identity(ObservationClass::Timer {
        timer_id: 99,
        deadline_ns: 5,
    });
    let before = handle.authority().ownership_report();
    assert_eq!(
        handle.admit_observation(&mut turn, &work, identity),
        Err(AuthorityError::TimerAuthorityRequired)
    );
    assert_eq!(handle.authority().ownership_report(), before);
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &qa_timer_frame(5, identity),
        AuthorityError::TimerAuthorityRequired,
    );
    assert!(matches!(
        handle.command(
            &mut turn,
            identity.stream,
            identity.epoch,
            CommandKind::SendText {
                text: "ping".to_owned()
            },
            &work
        ),
        Err(AuthorityError::TimerAuthorityRequired)
    ));
    assert_eq!(handle.authority().ownership_report(), before);
    // Rejecting the forged Timer preserves a caller's rightful unused W.
    let raw_identity = qa_original_identity(ObservationClass::Raw);
    handle
        .admit_observation(&mut turn, &work, raw_identity)
        .unwrap();
    work.set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    sink.persist_owned(&mut turn, &qa_raw(5, raw_identity, false), gate, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    assert!(matches!(
        handle.command(
            &mut turn,
            identity.stream,
            identity.epoch,
            CommandKind::SendText {
                text: "ping".to_owned()
            },
            &work
        ),
        Err(AuthorityError::TimerAuthorityRequired)
    ));
    assert_eq!(owner.sink_persist_calls(), 1);
    assert_eq!(read_all(&wal.0).0.len(), 5);
}

#[test]
fn timer_a_t04_durable_legacy_timer_and_generic_ping_routes_reject_preservingly() {
    qa_timer_t04_closed_legacy_routes(RecordingGate::Durable);
}
#[test]
fn timer_a_t04_written_legacy_timer_and_generic_ping_routes_reject_preservingly() {
    qa_timer_t04_closed_legacy_routes(RecordingGate::Written);
}

fn qa_timer_t05_cross_authority_and_replay(gate: RecordingGate) {
    let wal = TempWal::new("timer-t05-rightful");
    let foreign_wal = TempWal::new("timer-t05-foreign-numeric-equal");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let (foreign_owner, mut foreign_turn, foreign_handle, mut foreign_sink) =
        owner_with_fault(&foreign_wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    qa_timer_install_up(
        &foreign_handle,
        &mut foreign_turn,
        &mut foreign_sink,
        gate,
        binding().id,
        5,
    );
    let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let foreign_timer = qa_timer_due(
        &foreign_handle,
        &mut foreign_turn,
        binding().id,
        5 + QA_PING_NS,
    );
    assert_eq!(timer.identity(), foreign_timer.identity());
    assert_eq!(timer.owner().id(), foreign_timer.owner().id());
    let original = qa_timer_frame(6, timer.identity());
    let foreign_ledger = foreign_handle.authority().ownership_report();
    let foreign_status = foreign_owner.session_status();
    let foreign_calls = foreign_owner.sink_persist_calls();
    let foreign_physical = fs::read(&foreign_wal.0).unwrap();
    let before_foreign_extract = handle.authority().ownership_report();
    assert!(matches!(
        handle.take_timer_ping(&mut turn, foreign_timer.owner()),
        Err(AuthorityError::AuthorityMismatch)
    ));
    assert_eq!(
        handle.authority().ownership_report(),
        before_foreign_extract
    );
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        foreign_timer.owner(),
        gate,
        &original,
        AuthorityError::AuthorityMismatch,
    );
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut foreign_turn,
        &mut sink,
        timer.owner(),
        gate,
        &original,
        AuthorityError::AuthorityMismatch,
    );
    assert_eq!(
        foreign_handle.authority().ownership_report(),
        foreign_ledger
    );
    assert_eq!(foreign_owner.session_status(), foreign_status);
    assert_eq!(foreign_owner.sink_persist_calls(), foreign_calls);
    assert_eq!(fs::read(&foreign_wal.0).unwrap(), foreign_physical);
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_transport(6, timer.identity(), Transport::Down),
        AuthorityError::InvalidBinding,
    );
    sink.persist_owned(&mut turn, &original, gate, timer.owner())
        .unwrap();
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_frame(7, timer.identity()),
        AuthorityError::InvalidBinding,
    );
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    let ping = handle.take_timer_ping(&mut turn, timer.owner()).unwrap();
    drop(ping);
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_frame(7, timer.identity()),
        AuthorityError::InvalidOwner,
    );
    // Consumed metadata cannot be re-admitted as a new Timer obligation.
    let replacement = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    let before = handle.authority().ownership_report();
    assert_eq!(
        handle.admit_observation(&mut turn, &replacement, timer.identity()),
        Err(AuthorityError::TimerAuthorityRequired)
    );
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(owner.sink_persist_calls(), 2);
    assert_eq!(read_all(&wal.0).0.len(), 6);
    // The untouched foreign original remains independently recordable once.
    foreign_sink
        .persist_owned(&mut foreign_turn, &original, gate, foreign_timer.owner())
        .unwrap();
    qa_settle_once(
        &foreign_handle,
        &mut foreign_turn,
        &foreign_sink,
        foreign_timer.owner(),
    );
    drop(
        foreign_handle
            .take_timer_ping(&mut foreign_turn, foreign_timer.owner())
            .unwrap(),
    );
    assert_eq!(foreign_owner.sink_persist_calls(), 2);
    assert_eq!(read_all(&foreign_wal.0).0.len(), 6);
    let consumed_identity = timer.identity();
    let consumed_work_id = timer.owner().id();
    drop(replacement);
    drop(timer);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    // The retired original no longer has any capability. Recycled bounded
    // storage can only expose a fresh opaque original with new frontiers.
    let next = qa_timer_due(
        &handle,
        &mut turn,
        binding().id,
        5 + QA_PING_NS + QA_PONG_NS,
    );
    assert_eq!(next.kind(), TimerKind::Timeout);
    assert!(next.owner().id() > consumed_work_id);
    let ObservationClass::Timer {
        timer_id: old_id, ..
    } = consumed_identity.class
    else {
        unreachable!();
    };
    let ObservationClass::Timer {
        timer_id: new_id, ..
    } = next.identity().class
    else {
        unreachable!();
    };
    assert!(new_id > old_id);
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        next.owner(),
        gate,
        &qa_timer_frame(7, consumed_identity),
        AuthorityError::InvalidBinding,
    );
    let original_next = qa_timer_frame(7, next.identity());
    sink.persist_owned(&mut turn, &original_next, gate, next.owner())
        .unwrap();
    let original_down = qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &next);
    qa_settle_once(&handle, &mut turn, &sink, next.owner());
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        next.owner(),
        gate,
        &qa_timer_frame(9, consumed_identity),
        AuthorityError::InvalidOwner,
    );
    assert_eq!(&read_all(&wal.0).0[6..], &[original_next, original_down]);
    assert_eq!(owner.sink_persist_calls(), 4);
}

#[test]
fn timer_a_t05_durable_numeric_equal_foreign_capabilities_and_consumed_replays_preserve_originals()
{
    qa_timer_t05_cross_authority_and_replay(RecordingGate::Durable);
}
#[test]
fn timer_a_t05_written_numeric_equal_foreign_capabilities_and_consumed_replays_preserve_originals()
{
    qa_timer_t05_cross_authority_and_replay(RecordingGate::Written);
}

fn qa_timer_t06_due_and_capacity(gate: RecordingGate) {
    for two_scopes in [false, true] {
        let wal = TempWal::new("timer-t06-due-capacity");
        let (mut owner, mut turn, handle, mut sink) = if two_scopes {
            owner_two_scopes_with_gate(&wal.0, gate)
        } else {
            owner_with_fault(&wal.0, gate, None)
        };
        let before = handle.authority().ownership_report();
        assert!(matches!(
            handle
                .admit_due_timer(&mut turn, binding().id, qa_timer_stamp(u64::MAX))
                .unwrap(),
            TimerAdmission::NotDue
        ));
        assert_eq!(handle.authority().ownership_report(), before);
        qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
        if two_scopes {
            qa_timer_install_up(
                &handle,
                &mut turn,
                &mut sink,
                gate,
                positive(StreamId::new(2)),
                5,
            );
        }
        let baseline = handle.authority().ownership_report();
        let calls = owner.sink_persist_calls();
        assert!(matches!(
            handle
                .admit_due_timer(&mut turn, binding().id, qa_timer_stamp(4 + QA_PING_NS))
                .unwrap(),
            TimerAdmission::NotDue
        ));
        assert_eq!(handle.authority().ownership_report(), baseline);
        assert!(matches!(
            handle.admit_due_timer(
                &mut turn,
                positive(StreamId::new(3)),
                qa_timer_stamp(5 + QA_PING_NS)
            ),
            Err(AuthorityError::InvalidBinding)
        ));
        assert_eq!(handle.authority().ownership_report(), baseline);
        let mut filled = Vec::new();
        for _ in 0..baseline.work_limit {
            filled.push(
                handle
                    .reserve_work(&mut turn, WorkKind::PendingPlan)
                    .unwrap(),
            );
        }
        let full = handle.authority().ownership_report();
        let bytes = fs::read(&wal.0).unwrap();
        assert!(matches!(
            handle.admit_due_timer(&mut turn, binding().id, qa_timer_stamp(5 + QA_PING_NS)),
            Err(AuthorityError::WorkExhausted)
        ));
        assert_eq!(handle.authority().ownership_report(), full);
        assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        assert_eq!(owner.sink_persist_calls(), calls);
        drop(filled.pop().unwrap());
        let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
        let queued = handle.authority().ownership_report();
        for stamp in [5 + QA_PING_NS, 6 + QA_PING_NS, u64::MAX] {
            assert!(
                matches!(handle.admit_due_timer(&mut turn, binding().id, qa_timer_stamp(stamp)).unwrap(),
                TimerAdmission::AlreadyQueued { original_work_id } if original_work_id == timer.owner().id())
            );
            assert_eq!(handle.authority().ownership_report(), queued);
        }
        assert!(matches!(
            owner.register_supervisor(
                &mut turn,
                &[scope()],
                budget(),
                HeartbeatPolicy::SupervisorV2
            ),
            Err(OwnerError::Authority(AuthorityError::AlreadyRegistered))
                | Err(OwnerError::InvalidProfile(_))
        ));
        assert_eq!(handle.authority().ownership_report(), queued);
        sink.persist_owned(
            &mut turn,
            &qa_timer_frame(handle.prefix().next_record.get(), timer.identity()),
            gate,
            timer.owner(),
        )
        .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        drop(handle.take_timer_ping(&mut turn, timer.owner()).unwrap());
        drop(timer);
        drop(filled);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        if two_scopes {
            let neighbor = qa_timer_due(
                &handle,
                &mut turn,
                positive(StreamId::new(2)),
                5 + QA_PING_NS,
            );
            assert_eq!(neighbor.kind(), TimerKind::Ping);
            sink.persist_owned(
                &mut turn,
                &qa_timer_frame(handle.prefix().next_record.get(), neighbor.identity()),
                gate,
                neighbor.owner(),
            )
            .unwrap();
            qa_settle_once(&handle, &mut turn, &sink, neighbor.owner());
            drop(handle.take_timer_ping(&mut turn, neighbor.owner()).unwrap());
        }
    }
}

#[test]
fn timer_a_t06_durable_due_frontier_duplicate_and_capacity_rejection_keep_retryable_schedule() {
    qa_timer_t06_due_and_capacity(RecordingGate::Durable);
}
#[test]
fn timer_a_t06_written_due_frontier_duplicate_and_capacity_rejection_keep_retryable_schedule() {
    qa_timer_t06_due_and_capacity(RecordingGate::Written);
}

fn qa_timer_t07_public_time_overflow(gate: RecordingGate) {
    for obsolete in [false, true] {
        let wal = TempWal::new("timer-t07-time-overflow");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        qa_timer_install_up(
            &handle,
            &mut turn,
            &mut sink,
            gate,
            binding().id,
            u64::MAX - QA_PING_NS,
        );
        let timer = qa_timer_due(&handle, &mut turn, binding().id, u64::MAX);
        let original = qa_timer_frame(6, timer.identity());
        if obsolete {
            let ticket = owner.begin_finalization(&mut turn).unwrap();
            sink.persist_owned(&mut turn, &original, gate, timer.owner())
                .unwrap();
            assert_eq!(
                handle.timer_progress(&mut turn, timer.owner()).unwrap(),
                TimerProgressView::TimerOnlyObsolete {
                    timer_confirmed: true
                }
            );
            qa_settle_once(&handle, &mut turn, &sink, timer.owner());
            drop(timer);
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::Ready(_)
            ));
            assert_eq!(owner.sink_persist_calls(), 2);
            assert_eq!(read_all(&wal.0).0.len(), 6);
        } else {
            let before_calls = owner.sink_persist_calls();
            let prefix = handle.prefix();
            assert_eq!(
                sink.persist_owned(&mut turn, &original, gate, timer.owner()),
                Err(PersistBoundaryError::Authority(
                    AuthorityError::TimeOverflow
                ))
            );
            assert_eq!(owner.sink_persist_calls(), before_calls);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(read_all(&wal.0).0.len(), 5);
            assert!(owner.session_status().storage_stopped.is_some());
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                Err(AuthorityError::StorageStopped)
            );
        }
    }
}

#[test]
fn timer_a_t07_durable_active_ping_time_overflow_stops_and_obsolete_max_bypasses_arithmetic() {
    qa_timer_t07_public_time_overflow(RecordingGate::Durable);
}
#[test]
fn timer_a_t07_written_active_ping_time_overflow_stops_and_obsolete_max_bypasses_arithmetic() {
    qa_timer_t07_public_time_overflow(RecordingGate::Written);
}

fn qa_timer_t08_atomic_ping(gate: RecordingGate) {
    let wal = TempWal::new("timer-t08-atomic-ping");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let before = handle.authority().ownership_report();
    assert!(matches!(
        handle.take_timer_ping(&mut turn, timer.owner()),
        Err(AuthorityError::PingNotReady)
    ));
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, timer.owner(), None),
        Err(AuthorityError::NotQuiescent)
    );
    let aliases = [
        timer.owner().share().unwrap(),
        timer.owner().share().unwrap(),
        timer.owner().share().unwrap(),
    ];
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_frame(6, timer.identity()),
        AuthorityError::WorkShareExhausted,
    );
    assert_eq!(
        handle.timer_progress(&mut turn, timer.owner()).unwrap(),
        TimerProgressView::Unselected
    );
    drop(aliases);
    sink.persist_owned(
        &mut turn,
        &qa_timer_frame(6, timer.identity()),
        gate,
        timer.owner(),
    )
    .unwrap();
    assert_eq!(
        handle.timer_progress(&mut turn, timer.owner()).unwrap(),
        TimerProgressView::TimerOnlyPing {
            timer_confirmed: true,
            ping_taken: false
        }
    );
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    let minted_before = handle.authority().ownership_report();
    assert!(matches!(
        handle.command(
            &mut turn,
            timer.identity().stream,
            timer.identity().epoch,
            CommandKind::SendText {
                text: "ping".to_owned()
            },
            timer.owner()
        ),
        Err(AuthorityError::TimerAuthorityRequired)
    ));
    assert_eq!(handle.authority().ownership_report(), minted_before);
    let alias = timer.owner().share().unwrap();
    drop(timer);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    let command = handle.take_timer_ping(&mut turn, &alias).unwrap();
    let after_take = handle.authority().ownership_report();
    assert!(matches!(
        handle.take_timer_ping(&mut turn, &alias),
        Err(AuthorityError::PingAlreadyTaken)
    ));
    assert_eq!(handle.authority().ownership_report(), after_take);
    assert_eq!(
        handle.timer_progress(&mut turn, &alias).unwrap(),
        TimerProgressView::TimerOnlyPing {
            timer_confirmed: true,
            ping_taken: true
        }
    );
    drop(alias);
    assert_eq!(handle.authority().ownership_report().work_used, 1);
    let mut effects = 0;
    assert!(matches!(
        owner.dispatch(&mut turn, command, |view| {
            effects += 1;
            assert_eq!(
                view.kind,
                &CommandKind::SendText {
                    text: "ping".to_owned()
                }
            );
            Err::<(), _>("ambiguous Ping")
        }),
        DispatchReport::DispatchFailed {
            effect: AmbiguousEffect::Unknown,
            ..
        }
    ));
    assert_eq!(effects, 1);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    assert_eq!(read_all(&wal.0).0.len(), 6);
    qa_timer_t08_ping_faults(gate);
}

fn qa_timer_t08_ping_faults(gate: RecordingGate) {
    for variant in 0..if gate == RecordingGate::Durable { 3 } else { 2 } {
        let wal = TempWal::new("timer-t08-ping-receipt-fault");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
        let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
        let original = qa_timer_frame(6, timer.identity());
        let error = PersistError::typed(PersistErrorKind::Io, "original Ping receipt fault");
        let kind = match variant {
            0 => SinkFaultKind::BeforeWrite(error),
            1 => SinkFaultKind::ReceiptMismatch { through: record(5) },
            2 => SinkFaultKind::WeakGate {
                achieved: RecordingGate::Flushed,
            },
            _ => unreachable!(),
        };
        owner
            .set_sink_fault(
                &mut turn,
                Some(SinkFault {
                    at: record(6),
                    kind,
                }),
            )
            .unwrap();
        let prefix = handle.prefix();
        let calls = owner.sink_persist_calls();
        let actual = sink.persist_owned(&mut turn, &original, gate, timer.owner());
        match kind {
            SinkFaultKind::BeforeWrite(_) => {
                assert_eq!(actual, Err(PersistBoundaryError::Persistence(error)))
            }
            SinkFaultKind::ReceiptMismatch { through } => assert_eq!(
                actual,
                Err(PersistBoundaryError::ReceiptMismatch {
                    expected: record(6),
                    actual: through
                })
            ),
            SinkFaultKind::WeakGate { achieved } => assert_eq!(
                actual,
                Err(PersistBoundaryError::WeakGate {
                    required: gate,
                    achieved
                })
            ),
        }
        assert_eq!(owner.sink_persist_calls(), calls + 1);
        assert_eq!(handle.prefix(), prefix);
        assert!(owner.session_status().storage_stopped.is_some());
        assert!(matches!(
            handle.take_timer_ping(&mut turn, timer.owner()),
            Err(AuthorityError::CommandRevoked)
        ));
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, timer.owner(), None),
            Err(AuthorityError::StorageStopped)
        );
        assert_eq!(handle.authority().ownership_report().work_used, 1);
        assert_eq!(
            sink.persist_owned(&mut turn, &original, gate, timer.owner()),
            Err(PersistBoundaryError::Authority(
                AuthorityError::StorageStopped
            ))
        );
        assert_eq!(owner.sink_persist_calls(), calls + 1);
        assert!(matches!(
            owner.close_diagnostic(&mut turn).outcome,
            Ok(DiagnosticCloseState::Closed)
        ));
        let (records, report) = read_all(&wal.0);
        assert_eq!(records.len(), 5 + usize::from(variant != 0));
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(
            !records
                .iter()
                .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
        );
    }
}

#[test]
fn timer_a_t08_durable_exact_ping_receipt_retains_one_same_work_one_shot_ambiguous_effect() {
    qa_timer_t08_atomic_ping(RecordingGate::Durable);
}
#[test]
fn timer_a_t08_written_exact_ping_receipt_retains_one_same_work_one_shot_ambiguous_effect() {
    qa_timer_t08_atomic_ping(RecordingGate::Written);
}

fn qa_timer_t09_original_deadline_and_revocation(gate: RecordingGate) {
    for route in 0..6 {
        let wal = TempWal::new("timer-t09-delayed-revocation");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 100);
        let observed = 100 + QA_PING_NS + 1_000;
        let timer = qa_timer_due(&handle, &mut turn, binding().id, observed);
        let ObservationClass::Timer { deadline_ns, .. } = timer.identity().class else {
            unreachable!();
        };
        assert_eq!(deadline_ns, 100 + QA_PING_NS);
        sink.persist_owned(
            &mut turn,
            &qa_timer_frame(6, timer.identity()),
            gate,
            timer.owner(),
        )
        .unwrap();
        let ping = handle.take_timer_ping(&mut turn, timer.owner()).unwrap();
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        let before_due = handle.authority().ownership_report();
        assert!(matches!(
            handle
                .admit_due_timer(
                    &mut turn,
                    binding().id,
                    qa_timer_stamp(observed + QA_PONG_NS - 1)
                )
                .unwrap(),
            TimerAdmission::NotDue
        ));
        assert_eq!(handle.authority().ownership_report(), before_due);
        match route {
            0..=2 => {
                let class = [
                    ObservationClass::Pong,
                    ObservationClass::Connected,
                    ObservationClass::Disconnected,
                ][route];
                let mut identity = qa_original_identity(class);
                identity.stamp = qa_timer_stamp(observed + 1);
                let received = qa_admit(&handle, &mut turn, identity);
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_transport(
                        7,
                        identity,
                        if class == ObservationClass::Disconnected {
                            Transport::Down
                        } else {
                            Transport::Up
                        },
                    ),
                    gate,
                    &received,
                )
                .unwrap();
                qa_settle_once(&handle, &mut turn, &sink, &received);
            }
            3 => {
                let _ticket = owner.begin_finalization(&mut turn).unwrap();
            }
            4 => {
                let termination = handle.terminate(&mut turn, terminal()).unwrap();
                drop(termination.close);
            }
            5 => {
                let timeout = qa_timer_due(&handle, &mut turn, binding().id, observed + QA_PONG_NS);
                assert_eq!(timeout.kind(), TimerKind::Timeout);
                let ObservationClass::Timer { deadline_ns, .. } = timeout.identity().class else {
                    unreachable!();
                };
                assert_eq!(deadline_ns, observed + QA_PONG_NS);
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_frame(7, timeout.identity()),
                    gate,
                    timeout.owner(),
                )
                .unwrap();
                qa_timer_selected_close(&handle, &mut turn, timeout.owner());
                qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timeout);
                qa_settle_once(&handle, &mut turn, &sink, timeout.owner());
            }
            _ => unreachable!(),
        }
        let mut effects = 0;
        assert!(matches!(
            owner.dispatch(&mut turn, ping, |_| {
                effects += 1;
                Ok::<_, ()>(())
            }),
            DispatchReport::Revoked(AuthorityError::CommandRevoked)
        ));
        assert_eq!(effects, 0);
        assert_eq!(
            handle.timer_progress(&mut turn, timer.owner()).unwrap(),
            TimerProgressView::TimerOnlyPing {
                timer_confirmed: true,
                ping_taken: true
            }
        );
        assert_eq!(
            read_all(&wal.0).1.status,
            ArchiveStatus::ValidPrefixIncomplete
        );
    }
}

#[test]
fn timer_a_t09_durable_original_stamp_deadlines_and_six_held_ping_revocations() {
    qa_timer_t09_original_deadline_and_revocation(RecordingGate::Durable);
}
#[test]
fn timer_a_t09_written_original_stamp_deadlines_and_six_held_ping_revocations() {
    qa_timer_t09_original_deadline_and_revocation(RecordingGate::Written);
}

fn qa_timer_failure_marker(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
) {
    let mut marker = failed_marker();
    marker.record_no = handle.prefix().next_record;
    let Record::Control(ControlRecord {
        value: Control::Recording(evidence),
        ..
    }) = &mut marker.value
    else {
        unreachable!();
    };
    evidence.kind = if gate == RecordingGate::Durable {
        WatermarkKind::Durable
    } else {
        WatermarkKind::Written
    };
    evidence.through = handle.trusted_watermark(evidence.kind);
    sink.persist(turn, &marker, gate).unwrap();
    handle
        .authority()
        .marker_confirmed(turn, marker.record_no)
        .unwrap();
}

fn qa_timer_t10_lifecycle(gate: RecordingGate) {
    // Closing classifies only the still-unselected Timer as obsolete. A plan
    // already frozen by its exact Timer receipt must still obtain original Down.
    for selected_timeout in [false, true] {
        let wal = TempWal::new("timer-t10-closing");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let timer = if selected_timeout {
            qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id)
        } else {
            qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
            qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS)
        };
        let original = qa_timer_frame(handle.prefix().next_record.get(), timer.identity());
        let close = if selected_timeout {
            sink.persist_owned(&mut turn, &original, gate, timer.owner())
                .unwrap();
            Some(qa_timer_selected_close(&handle, &mut turn, timer.owner()))
        } else {
            None
        };
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert_eq!(owner.session_status().lifecycle, SessionLifecycle::Closing);
        assert!(matches!(
            handle.admit_due_timer(&mut turn, binding().id, qa_timer_stamp(u64::MAX)),
            Err(AuthorityError::SessionClosing)
        ));
        if selected_timeout {
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                Err(AuthorityError::NotQuiescent)
            );
            qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        } else {
            sink.persist_owned(&mut turn, &original, gate, timer.owner())
                .unwrap();
            assert_eq!(
                handle.timer_progress(&mut turn, timer.owner()).unwrap(),
                TimerProgressView::TimerOnlyObsolete {
                    timer_confirmed: true
                }
            );
            assert!(matches!(
                handle.take_timer_ping(&mut turn, timer.owner()),
                Err(AuthorityError::CommandRevoked)
            ));
        }
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        if let Some(close) = close {
            timer
                .owner()
                .set_kind(&mut turn, WorkKind::PendingPlan)
                .unwrap();
            assert_eq!(
                handle.retain_generated_plan(&mut turn, timer.owner()),
                Err(AuthorityError::SessionClosing)
            );
            let lease = leased(owner.reclaim_close(&mut turn, close));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
        }
        drop(timer);
        let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("settled admitted drain expected");
        };
        owner.finalize(&mut turn, &mut proof).unwrap();
        assert_eq!(
            owner.session_status().lifecycle,
            SessionLifecycle::Finalized
        );
        assert!(matches!(
            handle.admit_due_timer(&mut turn, binding().id, qa_timer_stamp(u64::MAX)),
            Err(AuthorityError::SessionClosed)
        ));
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::Complete);
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.value, Record::SegmentSeal(_)))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.value, Record::ArchiveSeal(_)))
                .count(),
            1
        );
    }
}

fn qa_timer_t10_diagnostic_neighbor(gate: RecordingGate) {
    // FailedDiagnostic keeps a healthy neighbor's Timer and transport effects
    // lawful while the failed scope's original unselected Timer becomes obsolete.
    let wal = TempWal::new("timer-t10-diagnostic-neighbor");
    let (mut owner, mut turn, handle, mut sink) = owner_two_scopes_with_gate(&wal.0, gate);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    qa_timer_install_up(
        &handle,
        &mut turn,
        &mut sink,
        gate,
        positive(StreamId::new(2)),
        5,
    );
    let failed_timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let termination = handle.terminate(&mut turn, terminal()).unwrap();
    let failed_close = termination.close_owner;
    drop(termination.close);
    sink.persist_owned(
        &mut turn,
        &qa_timer_frame(9, failed_timer.identity()),
        gate,
        failed_timer.owner(),
    )
    .unwrap();
    assert_eq!(
        handle
            .timer_progress(&mut turn, failed_timer.owner())
            .unwrap(),
        TimerProgressView::TimerOnlyObsolete {
            timer_confirmed: true
        }
    );
    qa_settle_once(&handle, &mut turn, &sink, failed_timer.owner());
    qa_timer_failure_marker(&handle, &mut turn, &mut sink, gate);
    let neighbor = qa_timer_due(
        &handle,
        &mut turn,
        positive(StreamId::new(2)),
        5 + QA_PING_NS,
    );
    sink.persist_owned(
        &mut turn,
        &qa_timer_frame(11, neighbor.identity()),
        gate,
        neighbor.owner(),
    )
    .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, neighbor.owner());
    let ping = handle.take_timer_ping(&mut turn, neighbor.owner()).unwrap();
    assert!(matches!(
        owner.dispatch(&mut turn, ping, |view| {
            assert_eq!(view.stream, positive(StreamId::new(2)));
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert!(matches!(
        owner.begin_finalization(&mut turn),
        Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
    ));
    drop(failed_timer);
    drop(neighbor);
    let report = owner.close_diagnostic(&mut turn);
    assert!(matches!(report.outcome, Ok(DiagnosticCloseState::Closed)));
    assert!(report.physical_report.descriptor_closed);
    assert!(matches!(
        handle.admit_due_timer(
            &mut turn,
            positive(StreamId::new(2)),
            qa_timer_stamp(u64::MAX)
        ),
        Err(AuthorityError::SessionClosed)
    ));
    let lease = leased(owner.reclaim_close(&mut turn, failed_close.clone()));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert!(matches!(
        owner.reclaim_close(&mut turn, failed_close),
        CloseLeaseReport::AlreadySettled
    ));
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

fn qa_timer_t10_diagnostic_owned_drain(gate: RecordingGate) {
    // DiagnosticClosing drains an already-admitted obsolete Timer before the
    // descriptor closes, with no invented Down or generated cancellation.
    let wal = TempWal::new("timer-t10-diagnostic-owned-drain");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let termination = handle.terminate(&mut turn, terminal()).unwrap();
    drop(termination.close);
    assert!(matches!(
        owner.close_diagnostic(&mut turn).outcome,
        Ok(DiagnosticCloseState::Closing)
    ));
    // R1: the original pre-cut Timer drains before the fixed failure marker.
    sink.persist_owned(
        &mut turn,
        &qa_timer_frame(6, timer.identity()),
        gate,
        timer.owner(),
    )
    .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    drop(timer);
    qa_timer_failure_marker(&handle, &mut turn, &mut sink, gate);
    assert!(matches!(
        owner.close_diagnostic(&mut turn).outcome,
        Ok(DiagnosticCloseState::Closed)
    ));
    assert_eq!(read_all(&wal.0).0.len(), 7);
    let wal = TempWal::new("timer-t10-diagnostic-frozen-timeout");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
    let original_timer = qa_timer_frame(7, timer.identity());
    sink.persist_owned(&mut turn, &original_timer, gate, timer.owner())
        .unwrap();
    let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
    let termination = handle.terminate(&mut turn, terminal()).unwrap();
    assert_eq!(termination.close_owner, close);
    drop(termination.close);
    assert!(matches!(
        owner.close_diagnostic(&mut turn).outcome,
        Ok(DiagnosticCloseState::Closing)
    ));
    let original_down = qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    drop(timer);
    qa_timer_failure_marker(&handle, &mut turn, &mut sink, gate);
    let closed = owner.close_diagnostic(&mut turn);
    assert!(matches!(closed.outcome, Ok(DiagnosticCloseState::Closed)));
    assert!(closed.physical_report.descriptor_closed);
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::AlreadySettled
    ));
    assert_eq!(&read_all(&wal.0).0[6..8], &[original_timer, original_down]);
    assert_eq!(read_all(&wal.0).0.len(), 9);
}

#[test]
fn timer_a_t10_durable_closing_original_drain_preserves_frozen_timeout_and_finalizes_once() {
    qa_timer_t10_lifecycle(RecordingGate::Durable);
}
#[test]
fn timer_a_t10_written_closing_original_drain_preserves_frozen_timeout_and_finalizes_once() {
    qa_timer_t10_lifecycle(RecordingGate::Written);
}

#[test]
fn timer_a_t10_durable_diagnostic_healthy_neighbor_timer_remains_lawful_then_descriptor_close() {
    qa_timer_t10_diagnostic_neighbor(RecordingGate::Durable);
}
#[test]
fn timer_a_t10_written_diagnostic_healthy_neighbor_timer_remains_lawful_then_descriptor_close() {
    qa_timer_t10_diagnostic_neighbor(RecordingGate::Written);
}
#[test]
fn timer_a_t10_durable_diagnostic_closing_drains_original_pre_cut_obsolete_and_frozen_timeout() {
    qa_timer_t10_diagnostic_owned_drain(RecordingGate::Durable);
}
#[test]
fn timer_a_t10_written_diagnostic_closing_drains_original_pre_cut_obsolete_and_frozen_timeout() {
    qa_timer_t10_diagnostic_owned_drain(RecordingGate::Written);
}

fn qa_timer_t10_preowned_partial_h1_closing(gate: RecordingGate) {
    let wal = TempWal::new("timer-t10-preowned-partial-h1");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
    let identity = timer.identity();
    sink.persist_owned(&mut turn, &qa_timer_frame(7, identity), gate, timer.owner())
        .unwrap();
    let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
    qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    timer
        .owner()
        .set_kind(&mut turn, WorkKind::PendingPlan)
        .unwrap();
    handle
        .retain_generated_plan(&mut turn, timer.owner())
        .unwrap();
    let lease = leased(owner.reclaim_close(&mut turn, close));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    let bound = binding();
    let connection = EpochChange::Connection {
        owner: bound.connection_id,
        expected: bound.tag.connection,
        next: positive(ConnectionEpoch::new(2)),
    };
    let subscription = EpochChange::Subscription {
        owner: bound.id,
        expected: bound.tag.subscription,
        next: positive(SubscriptionEpoch::new(2)),
    };
    let book = EpochChange::Book {
        owner: bound.book_id.unwrap(),
        expected: bound.tag.book.unwrap(),
        next: positive(BookEpoch::new(2)),
    };
    let connection_record = qa_epoch(9, identity, connection);
    sink.persist_owned(&mut turn, &connection_record, gate, timer.owner())
        .unwrap();
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let ledger = handle.authority().ownership_report();
    let prefix = handle.prefix();
    let calls = owner.sink_persist_calls();
    let bytes = fs::read(&wal.0).unwrap();
    assert_eq!(
        handle.cancel_generated_plan(&mut turn, timer.owner()),
        Err(AuthorityError::NotQuiescent)
    );
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.sink_persist_calls(), calls);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    let subscription_record = qa_epoch(10, identity, subscription);
    let book_record = qa_epoch(11, identity, book);
    sink.persist_owned(&mut turn, &subscription_record, gate, timer.owner())
        .unwrap();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, timer.owner(), None),
        Err(AuthorityError::NotQuiescent)
    );
    sink.persist_owned(&mut turn, &book_record, gate, timer.owner())
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    handle
        .authority()
        .advance_epoch(
            &mut turn,
            identity.stream,
            identity.epoch,
            positive(ConnectionEpoch::new(2)),
        )
        .unwrap();
    assert!(matches!(
        handle.admit_due_timer(&mut turn, identity.stream, qa_timer_stamp(u64::MAX)),
        Err(AuthorityError::SessionClosing)
    ));
    drop(timer);
    let QuiescenceReport::Ready(_proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("owned H1 exact stages drained before proof");
    };
    assert_eq!(
        &read_all(&wal.0).0[8..],
        &[connection_record, subscription_record, book_record]
    );
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

#[test]
fn timer_a_t10_durable_preowned_partial_h1_drains_during_closing_without_false_cancellation() {
    qa_timer_t10_preowned_partial_h1_closing(RecordingGate::Durable);
}
#[test]
fn timer_a_t10_written_preowned_partial_h1_drains_during_closing_without_false_cancellation() {
    qa_timer_t10_preowned_partial_h1_closing(RecordingGate::Written);
}

fn qa_timer_t11_close_readiness_and_recovery(gate: RecordingGate) {
    qa_timer_t11_conflicting_reservation(gate);
    for terminal_after_selection in [false, true] {
        let wal = TempWal::new("timer-t11-close-readiness");
        let foreign_wal = TempWal::new("timer-t11-foreign-close");
        let (mut owner, mut turn, handle, mut sink) = if terminal_after_selection {
            owner_two_scopes_with_gate(&wal.0, gate)
        } else {
            owner_with_fault(&wal.0, gate, None)
        };
        let (mut foreign_owner, mut foreign_turn, foreign_handle, _foreign_sink) =
            owner_with_fault(&foreign_wal.0, gate, None);
        let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
        assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
        sink.persist_owned(
            &mut turn,
            &qa_timer_frame(handle.prefix().next_record.get(), timer.identity()),
            gate,
            timer.owner(),
        )
        .unwrap();
        let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
        assert_eq!(
            handle
                .mandatory_close(
                    &mut turn,
                    binding().id,
                    binding().tag.connection,
                    Some(timer.owner())
                )
                .unwrap(),
            close
        );
        let before = handle.authority().ownership_report();
        let bytes = fs::read(&wal.0).unwrap();
        let calls = owner.sink_persist_calls();
        for _ in 0..3 {
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::Rejected(AuthorityError::CloseNotReady)
            ));
            assert_eq!(handle.authority().ownership_report(), before);
            assert_eq!(owner.sink_persist_calls(), calls);
            assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        }
        if terminal_after_selection {
            let mut neighbor_identity = qa_original_identity(ObservationClass::Connected);
            neighbor_identity.stream = positive(StreamId::new(2));
            neighbor_identity.stamp = qa_timer_stamp(7);
            // Already-owned neighbor work is PreCut when the selected timeout
            // scope fails; it can lawfully drain before the fixed marker.
            let neighbor = qa_admit(&handle, &mut turn, neighbor_identity);
            let termination = handle.terminate(&mut turn, terminal()).unwrap();
            assert_eq!(termination.close_owner, close);
            drop(termination.close);
            let TimerProgressView::TimerThenDown {
                timer_confirmed,
                down_confirmed,
                close: view,
            } = handle.timer_progress(&mut turn, timer.owner()).unwrap()
            else {
                unreachable!();
            };
            assert!(timer_confirmed && !down_confirmed && view.ready);
            assert_eq!(view.owner, close);
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                Err(AuthorityError::NotQuiescent)
            );
            sink.persist_owned(
                &mut turn,
                &qa_timer_transport(
                    handle.prefix().next_record.get(),
                    neighbor_identity,
                    Transport::Up,
                ),
                gate,
                &neighbor,
            )
            .unwrap();
            qa_settle_once(&handle, &mut turn, &sink, &neighbor);
            drop(neighbor);
            assert_eq!(
                read_all(&wal.0)
                    .0
                    .iter()
                    .filter(|f| matches!(
                        f.value,
                        Record::Control(ControlRecord {
                            value: Control::Transport {
                                value: Transport::Down,
                                ..
                            },
                            ..
                        })
                    ))
                    .count(),
                0
            );
        } else {
            qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        }
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.reclaim_close(&mut turn, close.clone()),
            CloseLeaseReport::AlreadyLeased
        ));
        let foreign_before = foreign_handle.authority().ownership_report();
        let mut effects = 0;
        let command =
            match foreign_owner.dispatch(&mut foreign_turn, lease.into_command().unwrap(), |_| {
                effects += 1;
                Ok::<_, ()>(())
            }) {
                DispatchReport::Denied {
                    reason: AuthorityError::AuthorityMismatch,
                    command,
                } => command,
                other => panic!("same valid Close returned on foreign dispatch: {other:?}"),
            };
        assert_eq!(effects, 0);
        assert_eq!(
            foreign_handle.authority().ownership_report(),
            foreign_before
        );
        drop(command);
        assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.dispatch(&mut turn, lease.into_command().unwrap(), |_| Err::<(), _>(
                "ambiguous Close"
            )),
            DispatchReport::DispatchFailed {
                effect: AmbiguousEffect::Unknown,
                ..
            }
        ));
        assert_eq!(close_state(&owner, &close), Some(CloseState::Pending));
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert!(matches!(
            owner.reclaim_close(&mut turn, close.clone()),
            CloseLeaseReport::AlreadySettled
        ));
        if terminal_after_selection {
            qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        }
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        assert_eq!(
            handle
                .mandatory_close(
                    &mut turn,
                    binding().id,
                    binding().tag.connection,
                    Some(timer.owner())
                )
                .unwrap(),
            close
        );
        assert_eq!(
            read_all(&wal.0).0.len(),
            if terminal_after_selection { 11 } else { 8 }
        );
        assert_eq!(
            read_all(&wal.0).1.status,
            ArchiveStatus::ValidPrefixIncomplete
        );
    }
}

fn qa_timer_t11_conflicting_reservation(gate: RecordingGate) {
    let wal = TempWal::new("timer-t11-conflicting-live-close");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_arm_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
    let identity = qa_original_identity(ObservationClass::Raw);
    let other = qa_admit(&handle, &mut turn, identity);
    sink.persist_owned(&mut turn, &qa_raw(7, identity, false), gate, &other)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &other);
    let prior_close = handle
        .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&other))
        .unwrap();
    assert_eq!(prior_close.storage(), CloseStorage::WorkOwner(other.id()));
    let timer = qa_timer_due(
        &handle,
        &mut turn,
        binding().id,
        5 + QA_PING_NS + QA_PONG_NS,
    );
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_frame(8, timer.identity()),
        AuthorityError::TimerCloseConflict,
    );
    assert_eq!(
        handle.timer_progress(&mut turn, timer.owner()).unwrap(),
        TimerProgressView::Unselected
    );
    assert_eq!(owner.session_status().storage_stopped, None);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 1);
    assert_eq!(close_state(&owner, &prior_close), Some(CloseState::Pending));
    assert_eq!(read_all(&wal.0).0.len(), 7);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
}

#[test]
fn timer_a_t11_durable_same_timeout_close_readiness_foreign_drop_error_and_terminal_failsafe() {
    qa_timer_t11_close_readiness_and_recovery(RecordingGate::Durable);
}
#[test]
fn timer_a_t11_written_same_timeout_close_readiness_foreign_drop_error_and_terminal_failsafe() {
    qa_timer_t11_close_readiness_and_recovery(RecordingGate::Written);
}

fn qa_timer_t12_fault_matrix(gate: RecordingGate) {
    for stage in 0..2 {
        for variant in 0..if gate == RecordingGate::Durable { 3 } else { 2 } {
            let wal = TempWal::new("timer-t12-stage-fault");
            let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
            let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
            if stage == 1 {
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_frame(7, timer.identity()),
                    gate,
                    timer.owner(),
                )
                .unwrap();
            }
            let original = if stage == 0 {
                qa_timer_frame(7, timer.identity())
            } else {
                qa_timer_transport(8, timer.identity(), Transport::Down)
            };
            let mut replacement = original.clone();
            let Record::Control(control) = &mut replacement.value else {
                unreachable!();
            };
            control.context.unix_ns = LocalUnixNs::new(timer.identity().stamp.unix_ns + 1);
            qa_timer_reject(
                (&wal, &owner, &handle),
                &mut turn,
                &mut sink,
                timer.owner(),
                gate,
                &replacement,
                AuthorityError::InvalidBinding,
            );
            assert_eq!(owner.session_status().storage_stopped, None);
            let prefix = handle.prefix();
            let trusted_written = handle.trusted_watermark(WatermarkKind::Written);
            let trusted_durable = handle.trusted_watermark(WatermarkKind::Durable);
            let physical = fs::read(&wal.0).unwrap();
            let calls = owner.sink_persist_calls();
            let error = PersistError::typed(PersistErrorKind::Io, "original Timer fault");
            let kind = match variant {
                0 => SinkFaultKind::BeforeWrite(error),
                1 => SinkFaultKind::ReceiptMismatch {
                    through: record(original.record_no.get() - 1),
                },
                2 => SinkFaultKind::WeakGate {
                    achieved: RecordingGate::Flushed,
                },
                _ => unreachable!(),
            };
            owner
                .set_sink_fault(
                    &mut turn,
                    Some(SinkFault {
                        at: original.record_no,
                        kind,
                    }),
                )
                .unwrap();
            let actual = sink.persist_owned(&mut turn, &original, gate, timer.owner());
            match kind {
                SinkFaultKind::BeforeWrite(_) => {
                    assert_eq!(actual, Err(PersistBoundaryError::Persistence(error)))
                }
                SinkFaultKind::ReceiptMismatch { through } => assert_eq!(
                    actual,
                    Err(PersistBoundaryError::ReceiptMismatch {
                        expected: original.record_no,
                        actual: through
                    })
                ),
                SinkFaultKind::WeakGate { achieved } => assert_eq!(
                    actual,
                    Err(PersistBoundaryError::WeakGate {
                        required: gate,
                        achieved
                    })
                ),
            }
            assert_eq!(owner.sink_persist_calls(), calls + 1);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(
                handle.trusted_watermark(WatermarkKind::Written),
                trusted_written
            );
            assert_eq!(
                handle.trusted_watermark(WatermarkKind::Durable),
                trusted_durable
            );
            let stopped = owner
                .session_status()
                .storage_stopped
                .expect("backend invocation hard-stops storage");
            if variant == 0 {
                assert_eq!(stopped, error);
                assert_eq!(fs::read(&wal.0).unwrap(), physical);
            } else {
                assert!(fs::read(&wal.0).unwrap().len() > physical.len());
            }
            let TimerProgressView::TimerThenDown {
                timer_confirmed,
                down_confirmed,
                close,
            } = handle.timer_progress(&mut turn, timer.owner()).unwrap()
            else {
                unreachable!();
            };
            assert_eq!(timer_confirmed, stage == 1);
            assert!(!down_confirmed);
            assert!(close.ready);
            assert_eq!(
                close.owner.storage(),
                CloseStorage::WorkOwner(timer.owner().id())
            );
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                Err(AuthorityError::StorageStopped)
            );
            let after_fault = fs::read(&wal.0).unwrap();
            assert_eq!(
                sink.persist_owned(&mut turn, &original, gate, timer.owner()),
                Err(PersistBoundaryError::Authority(
                    AuthorityError::StorageStopped
                ))
            );
            assert_eq!(owner.sink_persist_calls(), calls + 1);
            assert_eq!(fs::read(&wal.0).unwrap(), after_fault);
            let closed = owner.close_diagnostic(&mut turn);
            assert!(matches!(closed.outcome, Ok(DiagnosticCloseState::Closed)));
            assert!(closed.physical_report.descriptor_closed);
            assert!(closed.physical_report.unconfirmed_suffix_possible);
            assert!(matches!(
                owner.begin_finalization(&mut turn),
                Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
            ));
            let lease = leased(owner.reclaim_close(&mut turn, close.owner.clone()));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.owner),
                CloseLeaseReport::AlreadySettled
            ));
            let (records, report) = read_all(&wal.0);
            assert_eq!(records.len(), 6 + stage + usize::from(variant != 0));
            assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
            assert!(
                !records
                    .iter()
                    .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
            );
        }
    }
}

#[test]
fn timer_a_t12_durable_both_stages_before_write_mismatch_and_weak_gate_keep_truthful_prefix_and_close()
 {
    qa_timer_t12_fault_matrix(RecordingGate::Durable);
}
#[test]
fn timer_a_t12_written_both_stages_before_write_and_mismatch_keep_truthful_prefix_and_close() {
    qa_timer_t12_fault_matrix(RecordingGate::Written);
}

fn qa_timer_t13_original_completion_and_h1(gate: RecordingGate) {
    let wal = TempWal::new("timer-t13-original-h1");
    let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
    let identity = timer.identity();
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_transport(7, identity, Transport::Down),
        AuthorityError::InvalidBinding,
    );
    for obsolete in [None, Some((identity.stream, identity.epoch))] {
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, timer.owner(), obsolete),
            Err(AuthorityError::NotQuiescent)
        );
    }
    let original = qa_timer_frame(7, identity);
    sink.persist_owned(&mut turn, &original, gate, timer.owner())
        .unwrap();
    let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, timer.owner(), None),
        Err(AuthorityError::NotQuiescent)
    );
    let mut historical = identity;
    historical.stamp = qa_timer_stamp(identity.stamp.monotonic_ns - 1);
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &qa_timer_transport(8, historical, Transport::Down),
        AuthorityError::InvalidBinding,
    );
    let down = qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    timer
        .owner()
        .set_kind(&mut turn, WorkKind::PendingPlan)
        .unwrap();
    handle
        .retain_generated_plan(&mut turn, timer.owner())
        .unwrap();
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, timer.owner(), None),
        Err(AuthorityError::NotQuiescent)
    );
    let bound = binding();
    let connection = EpochChange::Connection {
        owner: bound.connection_id,
        expected: bound.tag.connection,
        next: positive(ConnectionEpoch::new(2)),
    };
    let subscription = EpochChange::Subscription {
        owner: bound.id,
        expected: bound.tag.subscription,
        next: positive(SubscriptionEpoch::new(2)),
    };
    let book = EpochChange::Book {
        owner: bound.book_id.unwrap(),
        expected: bound.tag.book.unwrap(),
        next: positive(BookEpoch::new(2)),
    };
    let before = handle.authority().ownership_report();
    let calls = owner.sink_persist_calls();
    assert_eq!(
        sink.persist_owned(
            &mut turn,
            &qa_epoch(9, identity, connection.clone()),
            gate,
            timer.owner()
        ),
        Err(PersistBoundaryError::Authority(
            AuthorityError::NotQuiescent
        ))
    );
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(owner.sink_persist_calls(), calls);
    let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            lease.into_command().unwrap(),
            |_| Ok::<_, ()>(())
        ),
        DispatchReport::Dispatched
    ));
    for (index, change) in [connection, subscription, book].into_iter().enumerate() {
        let stage = qa_epoch(9 + index as u64, identity, change);
        sink.persist_owned(&mut turn, &stage, gate, timer.owner())
            .unwrap();
        if index < 2 {
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, timer.owner(), None),
                Err(AuthorityError::NotQuiescent)
            );
        }
    }
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    handle
        .authority()
        .advance_epoch(
            &mut turn,
            identity.stream,
            identity.epoch,
            positive(ConnectionEpoch::new(2)),
        )
        .unwrap();
    let before_failure = read_all(&wal.0).0;
    assert_eq!(&before_failure[6..8], &[original, down]);
    assert_eq!(before_failure.len(), 11);
    let mut failure = terminal();
    failure.current_epoch = positive(ConnectionEpoch::new(2));
    failure.observed_tag.connection = failure.current_epoch;
    failure.observed_tag.subscription = positive(SubscriptionEpoch::new(2));
    failure.observed_tag.book = Some(positive(BookEpoch::new(2)));
    let termination = handle.terminate(&mut turn, failure).unwrap();
    assert_eq!(termination.close_owner.epoch(), failure.current_epoch);
    assert_eq!(read_all(&wal.0).0, before_failure);
    assert!(matches!(
        owner.reclaim_close(&mut turn, close),
        CloseLeaseReport::Rejected(AuthorityError::OwnerRetired)
    ));
    assert!(matches!(
        owner.begin_finalization(&mut turn),
        Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
    ));
}

#[test]
fn timer_a_t13_durable_original_timeout_stages_close_and_three_fresh_h1_epochs_are_required() {
    qa_timer_t13_original_completion_and_h1(RecordingGate::Durable);
}
#[test]
fn timer_a_t13_written_original_timeout_stages_close_and_three_fresh_h1_epochs_are_required() {
    qa_timer_t13_original_completion_and_h1(RecordingGate::Written);
}

fn qa_timer_t14_last_steward_abandonment(gate: RecordingGate) {
    qa_timer_t14_wrong_frame_does_not_synchronize_foreign_abandonment(gate);
    for phase in 0..4 {
        let wal = TempWal::new("timer-t14-last-steward");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let timer = if phase >= 2 {
            qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id)
        } else {
            qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
            qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS)
        };
        let identity = timer.identity();
        let work_id = timer.owner().id();
        if phase != 0 {
            sink.persist_owned(
                &mut turn,
                &qa_timer_frame(handle.prefix().next_record.get(), identity),
                gate,
                timer.owner(),
            )
            .unwrap();
        }
        let close = if phase >= 2 {
            Some(qa_timer_selected_close(&handle, &mut turn, timer.owner()))
        } else {
            None
        };
        if phase == 3 {
            qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        }
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        if phase == 2 {
            let original =
                qa_timer_transport(handle.prefix().next_record.get(), identity, Transport::Down);
            let error = PersistError::typed(PersistErrorKind::Io, "missing original Down");
            owner
                .set_sink_fault(
                    &mut turn,
                    Some(SinkFault {
                        at: original.record_no,
                        kind: SinkFaultKind::BeforeWrite(error),
                    }),
                )
                .unwrap();
            assert_eq!(
                sink.persist_owned(&mut turn, &original, gate, timer.owner()),
                Err(PersistBoundaryError::Persistence(error))
            );
        }
        drop(timer);
        if let Some(close) = close {
            assert_eq!(handle.authority().ownership_report().work_used, 1);
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            assert!(matches!(
                owner.dispatch(
                    &mut turn,
                    lease.into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
        }
        let abandoned = handle.authority().ownership_report();
        assert_eq!(abandoned.work_used, 1);
        assert_eq!(abandoned.abandoned_work, 1);
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
        ));
        let lost = owner
            .session_status()
            .first_abandonment
            .expect("retained original abandonment identity");
        assert_eq!(lost.work_id, work_id);
        assert_eq!(lost.identity, identity);
        assert!(matches!(
            owner.begin_finalization(&mut turn),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(
            !records
                .iter()
                .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
        );
    }
}

fn qa_timer_t14_wrong_frame_does_not_synchronize_foreign_abandonment(gate: RecordingGate) {
    let wal = TempWal::new("timer-t14-pure-reject-before-abandonment-sync");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    qa_timer_install_up(&handle, &mut turn, &mut sink, gate, binding().id, 5);
    let timer = qa_timer_due(&handle, &mut turn, binding().id, 5 + QA_PING_NS);
    let raw_identity = qa_original_identity(ObservationClass::Raw);
    let raw = qa_admit(&handle, &mut turn, raw_identity);
    let raw_id = raw.id();
    drop(raw);
    assert_eq!(handle.authority().ownership_report().abandoned_work, 1);
    assert_eq!(owner.session_status().storage_stopped, None);
    assert_eq!(owner.session_status().first_abandonment, None);
    let original = qa_timer_frame(6, timer.identity());
    let mut wrong = original.clone();
    let Record::Control(ControlRecord {
        value: Control::Timer { timer_id, .. },
        ..
    }) = &mut wrong.value
    else {
        unreachable!();
    };
    *timer_id += 1;
    qa_timer_reject(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        timer.owner(),
        gate,
        &wrong,
        AuthorityError::InvalidBinding,
    );
    assert_eq!(owner.session_status().storage_stopped, None);
    assert_eq!(owner.session_status().first_abandonment, None);
    let calls = owner.sink_persist_calls();
    let bytes = fs::read(&wal.0).unwrap();
    let prefix = handle.prefix();
    assert_eq!(
        sink.persist_owned(&mut turn, &original, gate, timer.owner()),
        Err(PersistBoundaryError::Authority(
            AuthorityError::StorageStopped
        ))
    );
    assert_eq!(owner.sink_persist_calls(), calls);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(
        handle.timer_progress(&mut turn, timer.owner()).unwrap(),
        TimerProgressView::Unselected
    );
    let abandonment = owner.session_status().first_abandonment.unwrap();
    assert_eq!(abandonment.work_id, raw_id);
    assert_eq!(abandonment.identity, raw_identity);
    assert_eq!(handle.authority().ownership_report().work_used, 2);
    assert_eq!(read_all(&wal.0).0.len(), 5);
}

#[test]
fn timer_a_t14_durable_last_steward_drop_at_four_timer_stages_keeps_original_abandoned_and_denies_seals()
 {
    qa_timer_t14_last_steward_abandonment(RecordingGate::Durable);
}
#[test]
fn timer_a_t14_written_last_steward_drop_at_four_timer_stages_keeps_original_abandoned_and_denies_seals()
 {
    qa_timer_t14_last_steward_abandonment(RecordingGate::Written);
}

fn qa_timer_t15_requested_allocation(gate: RecordingGate) {
    for cap in [5, 9] {
        let wal = TempWal::new("timer-t15-allocation");
        let probe = AllocationProbe::begin();
        let (mut owner, mut turn, handle, mut sink) = if cap == 5 {
            owner_with_fault(&wal.0, gate, None)
        } else {
            owner_two_scopes_with_gate(&wal.0, gate)
        };
        let metadata = owner.memory_report();
        let ledger = handle.authority().ownership_report();
        let computed_ceiling = ledger.metadata_ceiling_bytes
            + metadata.known_metadata_backing_bytes
            + metadata.registry_metadata_bound
            + metadata.encoder_workspace_bound
            + MAX_CAPTURE_PATH_BYTES
            + 8192;
        let initial_metadata_backing = ledger.metadata_backing_bytes;
        let initial_metadata_ceiling = ledger.metadata_ceiling_bytes;
        let mut monotonic = 5;
        for _ in 0..100 {
            for stream in 1..=if cap == 5 { 1 } else { 2 } {
                let stream = positive(StreamId::new(stream));
                qa_timer_install_up(&handle, &mut turn, &mut sink, gate, stream, monotonic);
                let timer = qa_timer_due(&handle, &mut turn, stream, monotonic + QA_PING_NS);
                let original = qa_timer_frame(handle.prefix().next_record.get(), timer.identity());
                let mut replacement = original.clone();
                let Record::Control(control) = &mut replacement.value else {
                    unreachable!();
                };
                control.context.monotonic_ns =
                    MonotonicNs::new(timer.identity().stamp.monotonic_ns + 1);
                let before = handle.authority().ownership_report();
                let calls = owner.sink_persist_calls();
                assert_eq!(
                    sink.persist_owned(&mut turn, &replacement, gate, timer.owner()),
                    Err(PersistBoundaryError::Authority(
                        AuthorityError::InvalidBinding
                    ))
                );
                assert_eq!(owner.sink_persist_calls(), calls);
                assert_eq!(handle.authority().ownership_report(), before);
                sink.persist_owned(&mut turn, &original, gate, timer.owner())
                    .unwrap();
                qa_settle_once(&handle, &mut turn, &sink, timer.owner());
                let command = handle.take_timer_ping(&mut turn, timer.owner()).unwrap();
                drop(timer);
                assert_eq!(handle.authority().ownership_report().work_used, 1);
                assert!(matches!(
                    owner.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
                    DispatchReport::Dispatched
                ));
                assert_eq!(handle.authority().ownership_report().work_used, 0);
                // An earlier received Pong revokes a due original Timer. Its
                // own exact Up commits before the original Timer-only record.
                let mut pong_identity = qa_original_identity(ObservationClass::Pong);
                pong_identity.stream = stream;
                pong_identity.stamp = qa_timer_stamp(monotonic + QA_PING_NS + QA_PONG_NS - 1);
                let pong = qa_admit(&handle, &mut turn, pong_identity);
                let obsolete = qa_timer_due(
                    &handle,
                    &mut turn,
                    stream,
                    monotonic + QA_PING_NS + QA_PONG_NS,
                );
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_transport(
                        handle.prefix().next_record.get(),
                        pong_identity,
                        Transport::Up,
                    ),
                    gate,
                    &pong,
                )
                .unwrap();
                qa_settle_once(&handle, &mut turn, &sink, &pong);
                drop(pong);
                sink.persist_owned(
                    &mut turn,
                    &qa_timer_frame(handle.prefix().next_record.get(), obsolete.identity()),
                    gate,
                    obsolete.owner(),
                )
                .unwrap();
                assert_eq!(
                    handle.timer_progress(&mut turn, obsolete.owner()).unwrap(),
                    TimerProgressView::TimerOnlyObsolete {
                        timer_confirmed: true
                    }
                );
                qa_settle_once(&handle, &mut turn, &sink, obsolete.owner());
                drop(obsolete);
                let current = handle.authority().ownership_report();
                assert_eq!(current.work_used, 0);
                assert_eq!(current.metadata_backing_bytes, initial_metadata_backing);
                assert_eq!(current.metadata_ceiling_bytes, initial_metadata_ceiling);
                assert!(current.inline_accounted_capacity_bytes <= current.inline_ceiling_bytes);
                monotonic += QA_PING_NS + QA_PONG_NS + 1;
            }
        }
        let timer = qa_timer_timeout(&handle, &mut turn, &mut sink, gate, binding().id);
        sink.persist_owned(
            &mut turn,
            &qa_timer_frame(handle.prefix().next_record.get(), timer.identity()),
            gate,
            timer.owner(),
        )
        .unwrap();
        let close = qa_timer_selected_close(&handle, &mut turn, timer.owner());
        qa_timer_commit_down(&handle, &mut turn, &mut sink, gate, &timer);
        qa_settle_once(&handle, &mut turn, &sink, timer.owner());
        drop(timer);
        let fixed = handle.authority().ownership_report();
        let fixed_live = probe.sample().live_requested_bytes;
        for _ in 0..100 {
            let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::AlreadyLeased
            ));
            drop(lease);
            assert_eq!(handle.authority().ownership_report(), fixed);
            assert_eq!(probe.sample().live_requested_bytes, fixed_live);
        }
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        let sample = probe.sample();
        assert!(!sample.unmatched_deallocation);
        assert!(sample.peak_requested_bytes <= computed_ceiling);
        drop(close);
        drop(sink);
        drop(handle);
        drop(turn);
        drop(owner);
        assert_eq!(probe.sample().live_requested_bytes, 0);
        assert!(!probe.sample().unmatched_deallocation);
        let peak = probe.sample().peak_requested_bytes;
        drop(probe);
        let (records, report) = read_all(&wal.0);
        assert_eq!(report.status, ArchiveStatus::ValidPrefixIncomplete);
        assert!(
            !records
                .iter()
                .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
        );
        eprintln!(
            "Timer A allocation gate={gate:?} cap={cap} metadata_backing={initial_metadata_backing} metadata_ceiling={initial_metadata_ceiling} peak={peak} computed_ceiling={computed_ceiling} teardown_live=0 loops=100"
        );
    }
}

#[test]
fn timer_a_t15_durable_actual_allocation_cap5_cap9_hundred_ping_obsolete_close_cycles_and_zero_teardown()
 {
    qa_timer_t15_requested_allocation(RecordingGate::Durable);
}
#[test]
fn timer_a_t15_written_actual_allocation_cap5_cap9_hundred_ping_obsolete_close_cycles_and_zero_teardown()
 {
    qa_timer_t15_requested_allocation(RecordingGate::Written);
}

// Independent preflight probe for ADR0003 section 15A.4 Finalized row.
// Public real owner/sink route; no fault or private-field mutation.
// Compilation and runtime are NOT_RUN in the Integrator environment.
#[test]
fn independent_qa_durable_finalized_archive_cannot_mint_or_dispatch_new_close() {
    let wal = TempWal::new("independent-finalized-close");
    let (mut owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let original = qa_raw(5, identity, false);
    sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    drop(work);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);

    // Healthy positive control proves real completion and both seals first.
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("healthy original must finalize");
    };
    owner.finalize(&mut turn, &mut proof).unwrap();
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    let (records, physical) = read_all(&wal.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    assert_eq!(records.len(), 7);
    assert_eq!(records[4], original);
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );

    let before_status = owner.session_status();
    let before_ownership = handle.authority().ownership_report();
    let before_close = owner.outstanding_close_owners();
    let before_unsettled = handle.authority().unsettled_summary();
    let before_prefix = handle.prefix();
    let before_bytes = fs::read(&wal.0).unwrap();
    let before_calls = owner.sink_persist_calls();
    assert_eq!(before_close.iter().count(), 0);

    // Preserve the actual result and inspect ALL public Close routes even when
    // minting incorrectly succeeds, so the log records the effect boundary.
    let mint = handle.mandatory_close(&mut turn, identity.stream, identity.epoch, None);
    let rejected = matches!(&mint, Err(AuthorityError::SessionClosed));
    let after_mint_close = owner.outstanding_close_owners();
    let mut effects = 0;
    eprintln!("late finalized mandatory_close={mint:?}; after_mint={after_mint_close:?}");
    if let Ok(close) = mint {
        let reclaim = owner.reclaim_close(&mut turn, close);
        eprintln!("late finalized reclaim={reclaim:?}");
        if let CloseLeaseReport::Leased(lease) = reclaim {
            let conversion = lease.into_command();
            eprintln!("late finalized conversion={conversion:?}");
            if let Ok(command) = conversion {
                let dispatch = owner.dispatch(&mut turn, command, |_| {
                    effects += 1;
                    Ok::<_, ()>(())
                });
                eprintln!("late finalized dispatch={dispatch:?}; effects={effects}");
            }
        }
    }

    assert_eq!(owner.session_status(), before_status);
    assert_eq!(handle.authority().ownership_report(), before_ownership);
    assert_eq!(handle.prefix(), before_prefix);
    assert_eq!(owner.sink_persist_calls(), before_calls);
    assert_eq!(fs::read(&wal.0).unwrap(), before_bytes);
    assert_eq!(read_all(&wal.0).0, records);
    assert_eq!(
        after_mint_close, before_close,
        "Finalized must not mint a new Close owner"
    );
    assert_eq!(owner.outstanding_close_owners(), before_close);
    assert_eq!(handle.authority().unsettled_summary(), before_unsettled);
    assert!(rejected, "late Close mint must return typed SessionClosed");
    assert_eq!(
        effects, 0,
        "Finalized must not dispatch a newly minted Close"
    );
}

fn qa_p2_finalized_close_preservation(with_original_close: bool) {
    let wal = TempWal::new("qa-p2-finalized-close");
    let foreign_wal = TempWal::new("qa-p2-finalized-close-foreign");
    let (mut owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let (foreign_owner, mut foreign_turn, foreign_handle, _foreign_sink) =
        owner_with_fault(&foreign_wal.0, RecordingGate::Durable, None);
    let identity = qa_original_identity(if with_original_close {
        ObservationClass::Disconnected
    } else {
        ObservationClass::Raw
    });
    let work = qa_admit(&handle, &mut turn, identity);
    let original = if with_original_close {
        qa_transport(5, identity, Transport::Down)
    } else {
        qa_raw(5, identity, false)
    };
    sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    let mut effects = 0;
    let original_close = if with_original_close {
        let close = qa_pending_down_close(&owner, identity, &work);
        let lease = leased(owner.reclaim_close(&mut turn, close.clone()));
        assert!(matches!(
            owner.dispatch(&mut turn, lease.into_command().unwrap(), |_| {
                effects += 1;
                Ok::<_, ()>(())
            }),
            DispatchReport::Dispatched
        ));
        assert_eq!(effects, 1, "the original ready Close callback succeeds");
        assert_eq!(
            handle
                .authority()
                .close_state(identity.stream, identity.epoch),
            Ok(CloseState::Settled)
        );
        Some(close)
    } else {
        None
    };
    drop(work);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("completed Durable input and original Close must quiesce")
    };
    owner.finalize(&mut turn, &mut proof).unwrap();
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    let (records, physical) = read_all(&wal.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    assert_eq!(physical.input_quality, Some(InputQuality::Unknown));
    assert_eq!(records.len(), 7);
    assert_eq!(records[4], original);
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(
                frame.value,
                Record::SegmentSeal(SegmentSeal { is_final: true, .. })
            ))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
    let status = owner.session_status();
    let ledger = handle.authority().ownership_report();
    let unsettled = handle.authority().unsettled_summary();
    let close_owners = owner.outstanding_close_owners();
    let prefix = handle.prefix();
    let watermarks = owner.watermarks();
    let bytes = fs::read(&wal.0).unwrap();
    let calls = owner.sink_persist_calls();
    let foreign_status = foreign_owner.session_status();
    let foreign_ledger = foreign_handle.authority().ownership_report();
    let foreign_unsettled = foreign_handle.authority().unsettled_summary();
    let foreign_close = foreign_owner.outstanding_close_owners();
    let foreign_prefix = foreign_handle.prefix();
    let foreign_watermarks = foreign_owner.watermarks();
    let foreign_bytes = fs::read(&foreign_wal.0).unwrap();
    let foreign_calls = foreign_owner.sink_persist_calls();
    macro_rules! unchanged {
        () => {
            assert_eq!(owner.session_status(), status);
            assert_eq!(handle.authority().ownership_report(), ledger);
            assert_eq!(handle.authority().unsettled_summary(), unsettled);
            assert_eq!(owner.outstanding_close_owners(), close_owners);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.watermarks(), watermarks);
            assert_eq!(owner.sink_persist_calls(), calls);
            assert_eq!(fs::read(&wal.0).unwrap(), bytes);
            assert_eq!(effects, usize::from(with_original_close));
            assert_eq!(foreign_owner.session_status(), foreign_status);
            assert_eq!(
                foreign_handle.authority().ownership_report(),
                foreign_ledger
            );
            assert_eq!(
                foreign_handle.authority().unsettled_summary(),
                foreign_unsettled
            );
            assert_eq!(foreign_owner.outstanding_close_owners(), foreign_close);
            assert_eq!(foreign_handle.prefix(), foreign_prefix);
            assert_eq!(foreign_owner.watermarks(), foreign_watermarks);
            assert_eq!(foreign_owner.sink_persist_calls(), foreign_calls);
            assert_eq!(fs::read(&foreign_wal.0).unwrap(), foreign_bytes);
        };
    }
    for _ in 0..100 {
        assert!(matches!(
            handle.mandatory_close(&mut turn, identity.stream, identity.epoch, None),
            Err(AuthorityError::SessionClosed)
        ));
        unchanged!();
        assert!(matches!(
            handle
                .authority()
                .mandatory_close(&mut turn, identity.stream, identity.epoch, None),
            Err(AuthorityError::SessionClosed)
        ));
        unchanged!();
        if let Some(close) = &original_close {
            assert!(matches!(
                owner.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::Rejected(AuthorityError::SessionClosed)
            ));
            unchanged!();
            assert!(matches!(
                handle.authority().reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::Rejected(AuthorityError::SessionClosed)
            ));
            assert_eq!(
                handle
                    .authority()
                    .close_state(identity.stream, identity.epoch),
                Ok(CloseState::Settled),
                "read-only settlement remains truthful without issuing a lease"
            );
            unchanged!();
        }
    }
    assert!(matches!(
        handle.mandatory_close(&mut foreign_turn, identity.stream, identity.epoch, None),
        Err(AuthorityError::AuthorityMismatch)
    ));
    unchanged!();
    assert!(matches!(
        handle.authority().mandatory_close(
            &mut foreign_turn,
            identity.stream,
            identity.epoch,
            None
        ),
        Err(AuthorityError::AuthorityMismatch)
    ));
    if let Some(close) = &original_close {
        assert!(matches!(
            owner.reclaim_close(&mut foreign_turn, close.clone()),
            CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
        ));
        assert!(matches!(
            handle
                .authority()
                .reclaim_close(&mut foreign_turn, close.clone()),
            CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
        ));
    }
    unchanged!();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    assert!(matches!(
        owner.finalize(&mut turn, &mut proof),
        Err(OwnerError::Authority(AuthorityError::ProofConsumed))
    ));
    unchanged!();
    assert_eq!(read_all(&wal.0), (records, physical));
}

#[test]
fn qa_p2_durable_finalized_repeated_handle_and_authority_mint_preserve_complete_archive() {
    qa_p2_finalized_close_preservation(false);
}

#[test]
fn qa_p2_durable_finalized_settled_original_close_cannot_reclaim_or_create_effect() {
    qa_p2_finalized_close_preservation(true);
}

// Independent public-handle F2 contract probe, actual filesystem Durable.
// The new received loss would follow an already-admitted control barrier.
// No production/private mutation, fabricated receipt or alternate writer.
fn independent_qa_public_gap_extension_probe(with_control_barrier: bool) {
    let wal = TempWal::new("independent-gap-control-barrier");
    let (owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let identity = qa_original_identity(ObservationClass::Gap);
    let gap_work = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    handle
        .admit_observation(&mut turn, &gap_work, identity)
        .unwrap();
    let control_identity = ObservationIdentity {
        stamp: ReceiveStamp {
            unix_ns: 6,
            monotonic_ns: 6,
        },
        ..qa_original_identity(ObservationClass::Connected)
    };
    let barrier = with_control_barrier.then(|| {
        let work = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle
            .admit_observation(&mut turn, &work, control_identity)
            .unwrap();
        work
    });
    let before = handle.authority().ownership_report();
    let before_status = owner.session_status();
    let before_prefix = handle.prefix();
    let before_calls = owner.sink_persist_calls();
    let before_bytes = fs::read(&wal.0).unwrap();
    let expanded = ObservationIdentity {
        attempts: Some((
            positive(CaptureAttemptNo::new(1)),
            positive(CaptureAttemptNo::new(2)),
        )),
        loss_count: Some(2),
        ..identity
    };
    let extension = handle.extend_gap_observation(&mut turn, &gap_work, expanded);
    let rejected = extension.is_err();
    eprintln!("F2 with_control_barrier={with_control_barrier}; extension={extension:?}");
    assert_eq!(handle.authority().ownership_report(), before);
    assert_eq!(owner.session_status(), before_status);
    assert_eq!(handle.prefix(), before_prefix);
    assert_eq!(owner.sink_persist_calls(), before_calls);
    assert_eq!(fs::read(&wal.0).unwrap(), before_bytes);

    // Observe the accepted identity at the real bound writer, then drain the
    // unchanged control. This avoids calling an accepted wrong mutation merely
    // hypothetical when its altered original can really enter the prefix.
    let accepted_identity = if extension.is_ok() {
        expanded
    } else {
        identity
    };
    let written_gap = qa_gap(5, accepted_identity, Reason::QueueOverflow);
    gap_work
        .set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    sink.persist_owned(&mut turn, &written_gap, RecordingGate::Durable, &gap_work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &gap_work);
    drop(gap_work);
    if let Some(control_work) = barrier {
        let original_control = qa_transport(6, control_identity, Transport::Up);
        control_work
            .set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        sink.persist_owned(
            &mut turn,
            &original_control,
            RecordingGate::Durable,
            &control_work,
        )
        .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &control_work);
        drop(control_work);
        assert_eq!(
            &read_all(&wal.0).0[4..],
            &[written_gap.clone(), original_control]
        );
    } else {
        assert_eq!(read_all(&wal.0).0[4], written_gap);
    }
    eprintln!(
        "F2 written_gap={written_gap:?}; physical={:?}; work_used={}",
        read_all(&wal.0).1.status,
        handle.authority().ownership_report().work_used
    );
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
    if with_control_barrier {
        assert!(
            rejected,
            "public F2 extension must reject after an intervening admitted control barrier"
        );
    } else {
        assert!(
            extension.is_ok(),
            "lawful contiguous same-tail GAP extension positive control"
        );
    }
}

#[test]
fn independent_qa_durable_public_gap_extension_rejects_intervening_admitted_control() {
    independent_qa_public_gap_extension_probe(true);
}

#[test]
fn independent_qa_durable_public_gap_extension_allows_lawful_same_tail_control() {
    independent_qa_public_gap_extension_probe(false);
}

// Corrective P2 evidence uses the accepted concrete filesystem Durable sink.
// No private state, fabricated receipt, alternate writer or weaker gate.
fn p2_queued_gap(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    identity: ObservationIdentity,
) -> WorkOwner {
    let work = handle
        .reserve_work(turn, WorkKind::QueuedObservation)
        .unwrap();
    handle.admit_observation(turn, &work, identity).unwrap();
    work
}

fn p2_expanded_gap(identity: ObservationIdentity, last: u64) -> ObservationIdentity {
    ObservationIdentity {
        attempts: Some((
            identity.attempts.unwrap().0,
            positive(CaptureAttemptNo::new(last)),
        )),
        loss_count: Some(last),
        ..identity
    }
}

fn p2_gap_reject_preserving(
    boundary: (&TempWal, &CaptureSessionOwner, &SupervisorSessionHandle),
    turn: &mut SessionTurn,
    work: &WorkOwner,
    expanded: ObservationIdentity,
) {
    let (wal, owner, handle) = boundary;
    let status = owner.session_status();
    let ledger = handle.authority().ownership_report();
    let unsettled = handle.authority().unsettled_summary();
    let close = owner.outstanding_close_owners();
    let prefix = handle.prefix();
    let calls = owner.sink_persist_calls();
    let bytes = fs::read(&wal.0).unwrap();
    let id = work.id();
    let cut = work.cut_side();
    for _ in 0..100 {
        assert_eq!(handle.gap_extension_eligible(turn, work), Ok(false));
        assert_eq!(
            handle.extend_gap_observation(turn, work, expanded),
            Err(AuthorityError::InvalidOwner)
        );
        assert_eq!(owner.session_status(), status);
        assert_eq!(handle.authority().ownership_report(), ledger);
        assert_eq!(handle.authority().unsettled_summary(), unsettled);
        assert_eq!(owner.outstanding_close_owners(), close);
        assert_eq!(handle.prefix(), prefix);
        assert_eq!(owner.sink_persist_calls(), calls);
        assert_eq!(work.id(), id);
        assert_eq!(work.cut_side(), cut);
    }
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
}

fn p2_write_original_gap_after_rejection(
    boundary: (&TempWal, &CaptureSessionOwner, &SupervisorSessionHandle),
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    work: &WorkOwner,
    original: ObservationIdentity,
) {
    let (wal, owner, handle) = boundary;
    work.set_kind(turn, WorkKind::InFlightObservation).unwrap();
    let number = handle.prefix().next_record.get();
    let expanded = qa_gap(number, p2_expanded_gap(original, 2), Reason::QueueOverflow);
    qa_reject_preserving(
        (wal, owner, handle),
        turn,
        sink,
        work,
        RecordingGate::Durable,
        &expanded,
    );
    let rightful = qa_gap(number, original, Reason::QueueOverflow);
    sink.persist_owned(turn, &rightful, RecordingGate::Durable, work)
        .unwrap();
    qa_settle_once(handle, turn, sink, work);
    assert_eq!(read_all(&wal.0).0.last(), Some(&rightful));
}

#[test]
fn p2_durable_gap_rejects_connected_pong_down_raw_and_due_timer_barriers() {
    for class in [
        ObservationClass::Connected,
        ObservationClass::Pong,
        ObservationClass::Disconnected,
        ObservationClass::Raw,
    ] {
        let wal = TempWal::new("p2-gap-public-barriers");
        let (mut owner, mut turn, handle, mut sink) =
            owner_with_fault(&wal.0, RecordingGate::Durable, None);
        let original = qa_original_identity(ObservationClass::Gap);
        let gap = p2_queued_gap(&handle, &mut turn, original);
        let identity = ObservationIdentity {
            stamp: ReceiveStamp {
                unix_ns: 6,
                monotonic_ns: 6,
            },
            // Gap attempt1 already accounts the loss. The later received Raw
            // is the genuine next attempt2, not a duplicate of the lost input.
            attempts: (class == ObservationClass::Raw).then_some((
                positive(CaptureAttemptNo::new(2)),
                positive(CaptureAttemptNo::new(2)),
            )),
            ..qa_original_identity(class)
        };
        let barrier = p2_queued_gap(&handle, &mut turn, identity);
        p2_gap_reject_preserving(
            (&wal, &owner, &handle),
            &mut turn,
            &gap,
            p2_expanded_gap(original, 2),
        );
        p2_write_original_gap_after_rejection(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &gap,
            original,
        );
        drop(gap);
        barrier
            .set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let frame = match class {
            ObservationClass::Raw => qa_raw(handle.prefix().next_record.get(), identity, false),
            ObservationClass::Disconnected => {
                qa_transport(handle.prefix().next_record.get(), identity, Transport::Down)
            }
            _ => qa_transport(handle.prefix().next_record.get(), identity, Transport::Up),
        };
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &barrier)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &barrier);
        drop(barrier);
        if class == ObservationClass::Disconnected {
            let close = owner
                .outstanding_close_owners()
                .iter()
                .next()
                .unwrap()
                .owner
                .clone();
            let command = leased(owner.reclaim_close(&mut turn, close))
                .into_command()
                .unwrap();
            let mut effects = 0;
            assert!(matches!(
                owner.dispatch(&mut turn, command, |_| {
                    effects += 1;
                    Ok::<_, ()>(())
                }),
                DispatchReport::Dispatched
            ));
            assert_eq!(effects, 1);
        }
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        assert_eq!(
            read_all(&wal.0).1.status,
            ArchiveStatus::ValidPrefixIncomplete
        );
        assert_eq!(read_all(&wal.0).0.len(), 6);
    }
    let wal = TempWal::new("p2-gap-due-timer-barrier");
    let (mut owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    qa_timer_install_up(
        &handle,
        &mut turn,
        &mut sink,
        RecordingGate::Durable,
        scope().stream,
        5,
    );
    let original = qa_original_identity(ObservationClass::Gap);
    let gap = p2_queued_gap(&handle, &mut turn, original);
    let timer = qa_timer_due(&handle, &mut turn, scope().stream, 5 + QA_PING_NS);
    assert_eq!(timer.kind(), TimerKind::Ping);
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    p2_write_original_gap_after_rejection(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &gap,
        original,
    );
    drop(gap);
    let frame = qa_timer_frame(handle.prefix().next_record.get(), timer.identity());
    sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, timer.owner())
        .unwrap();
    let command = handle.take_timer_ping(&mut turn, timer.owner()).unwrap();
    qa_settle_once(&handle, &mut turn, &sink, timer.owner());
    drop(timer);
    assert!(matches!(
        owner.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
        DispatchReport::Dispatched
    ));
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

#[test]
fn p2_durable_gap_global_tail_barrier_survives_neighbor_settlement_alias_drop_and_cell_reuse() {
    let wal = TempWal::new("p2-gap-neighbor-settled");
    let (owner, mut turn, handle, mut sink) =
        owner_two_scopes_with_gate(&wal.0, RecordingGate::Durable);
    let original = qa_original_identity(ObservationClass::Gap);
    let gap = p2_queued_gap(&handle, &mut turn, original);
    let neighbor_identity = ObservationIdentity {
        stream: positive(StreamId::new(2)),
        stamp: ReceiveStamp {
            unix_ns: 6,
            monotonic_ns: 6,
        },
        ..qa_original_identity(ObservationClass::Connected)
    };
    let neighbor = p2_queued_gap(&handle, &mut turn, neighbor_identity);
    neighbor
        .set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    let up = qa_timer_transport(
        handle.prefix().next_record.get(),
        neighbor_identity,
        Transport::Up,
    );
    // A1 remains scoped: the genuine neighbor actually progresses despite the
    // earlier queued Gap, while its admission removes GLOBAL F2 tail eligibility.
    sink.persist_owned(&mut turn, &up, RecordingGate::Durable, &neighbor)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &neighbor);
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    drop(neighbor);
    let unadmitted_reuse = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    drop(unadmitted_reuse);
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    p2_write_original_gap_after_rejection(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &gap,
        original,
    );
    drop(gap);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    let records = read_all(&wal.0).0;
    assert_eq!(
        &records[6..],
        &[up, qa_gap(8, original, Reason::QueueOverflow)]
    );
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

#[test]
fn p2_durable_gap_queued_tail_reserved_reverse_order_and_failed_capacity_do_not_block_f2() {
    let wal = TempWal::new("p2-gap-reservation-order");
    let (owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let reserved_before = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    let original = qa_original_identity(ObservationClass::Gap);
    let gap = p2_queued_gap(&handle, &mut turn, original);
    let reserved_after = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    assert_eq!(
        handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap_err(),
        AuthorityError::WorkExhausted
    );
    let id = gap.id();
    let cut = gap.cut_side();
    let ledger = handle.authority().ownership_report();
    let status = owner.session_status();
    let prefix = handle.prefix();
    let calls = owner.sink_persist_calls();
    let mut expanded = original;
    for last in 2..=33 {
        expanded = p2_expanded_gap(original, last);
        assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
        handle
            .extend_gap_observation(&mut turn, &gap, expanded)
            .unwrap();
        assert_eq!(gap.id(), id);
        assert_eq!(gap.cut_side(), cut);
        assert_eq!(handle.authority().ownership_report(), ledger);
        assert_eq!(owner.session_status(), status);
        assert_eq!(handle.prefix(), prefix);
        assert_eq!(owner.sink_persist_calls(), calls);
    }
    drop(reserved_before);
    drop(reserved_after);
    gap.set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    let final_gap = qa_gap(5, expanded, Reason::QueueOverflow);
    sink.persist_owned(&mut turn, &final_gap, RecordingGate::Durable, &gap)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &gap);
    drop(gap);
    assert_eq!(read_all(&wal.0).0[4], final_gap);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
}

#[test]
fn p2_durable_gap_rejection_does_not_reconcile_abandoned_barrier_or_change_failure_cut() {
    let wal = TempWal::new("p2-gap-no-reconciliation");
    let (owner, mut turn, handle, _sink) = owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let original = qa_original_identity(ObservationClass::Gap);
    let gap = p2_queued_gap(&handle, &mut turn, original);
    let abandoned = p2_queued_gap(
        &handle,
        &mut turn,
        qa_original_identity(ObservationClass::Connected),
    );
    drop(abandoned);
    assert!(!owner.session_status().failed);
    assert!(owner.session_status().first_abandonment.is_none());
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    assert!(!owner.session_status().failed);
    assert!(owner.session_status().first_abandonment.is_none());
    // Actual later reconciliation, rather than extension rejection, installs
    // the truthful original abandonment. No completion/proof/seal is claimed.
    handle
        .authority()
        .synchronize_obligations(&mut turn)
        .unwrap();
    assert!(owner.session_status().failed);
    assert!(owner.session_status().first_abandonment.is_some());
    p2_gap_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &gap,
        p2_expanded_gap(original, 2),
    );
    assert_eq!(read_all(&wal.0).0.len(), 4);
    assert_eq!(
        read_all(&wal.0).1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

fn p2_gap_requested_allocation(gate: RecordingGate) {
    for cap in [5, 9] {
        let wal = TempWal::new("p2-gap-allocation");
        let probe = AllocationProbe::begin();
        let (owner, mut turn, handle, mut sink) = if cap == 5 {
            owner_with_fault(&wal.0, gate, None)
        } else {
            owner_two_scopes_with_gate(&wal.0, gate)
        };
        let metadata = owner.memory_report();
        let ledger = handle.authority().ownership_report();
        let ceiling = ledger.metadata_ceiling_bytes
            + metadata.known_metadata_backing_bytes
            + metadata.registry_metadata_bound
            + metadata.encoder_workspace_bound
            + MAX_CAPTURE_PATH_BYTES
            + 8192;
        let identity = qa_original_identity(ObservationClass::Gap);
        let gap = p2_queued_gap(&handle, &mut turn, identity);
        let control_identity = ObservationIdentity {
            stream: positive(StreamId::new(if cap == 5 { 1 } else { 2 })),
            stamp: ReceiveStamp {
                unix_ns: 6,
                monotonic_ns: 6,
            },
            ..qa_original_identity(ObservationClass::Connected)
        };
        let barrier = p2_queued_gap(&handle, &mut turn, control_identity);
        let expanded = p2_expanded_gap(identity, 2);
        let status = owner.session_status();
        let ownership = handle.authority().ownership_report();
        let prefix = handle.prefix();
        let calls = owner.sink_persist_calls();
        let baseline = probe.sample().live_requested_bytes;
        for _ in 0..100 {
            assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
            assert_eq!(
                handle.extend_gap_observation(&mut turn, &gap, expanded),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(owner.session_status(), status);
            assert_eq!(handle.authority().ownership_report(), ownership);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.sink_persist_calls(), calls);
            assert_eq!(probe.sample().live_requested_bytes, baseline);
        }
        gap.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let original = qa_gap(
            handle.prefix().next_record.get(),
            identity,
            Reason::QueueOverflow,
        );
        sink.persist_owned(&mut turn, &original, gate, &gap)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &gap);
        drop(gap);
        barrier
            .set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let control = qa_timer_transport(
            handle.prefix().next_record.get(),
            control_identity,
            Transport::Up,
        );
        sink.persist_owned(&mut turn, &control, gate, &barrier)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &barrier);
        drop(barrier);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        let measured = probe.sample();
        assert!(!measured.unmatched_deallocation);
        assert!(measured.peak_requested_bytes <= ceiling);
        // Release test-owned frame payloads before measuring complete teardown.
        // The original GapTarget vector is part of requested allocation too.
        drop(original);
        drop(control);
        drop(sink);
        drop(handle);
        drop(turn);
        drop(owner);
        assert_eq!(probe.sample().live_requested_bytes, 0);
        assert!(!probe.sample().unmatched_deallocation);
        drop(probe);
        eprintln!(
            "p2 GAP requested allocation gate={gate:?} cap={cap}: rejects=100 retained_baseline={baseline} peak={} ceiling={ceiling} teardown_live=0",
            measured.peak_requested_bytes
        );
    }
}

#[test]
fn p2_written_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    p2_gap_requested_allocation(RecordingGate::Written);
}

#[test]
fn p2_flushed_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    p2_gap_requested_allocation(RecordingGate::Flushed);
}

#[test]
fn p2_durable_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    p2_gap_requested_allocation(RecordingGate::Durable);
}

// Fresh public Durable probes: existing owner-minted authority, no field mutation.
fn independent_qa_42_postcut_scope_gap(terminate_gap_scope: bool) {
    let wal = TempWal::new("independent-42-terminal-gap");
    let (mut owner, mut turn, handle, mut sink) =
        owner_two_scopes_with_gate(&wal.0, RecordingGate::Durable);
    let first = handle.terminate(&mut turn, terminal()).unwrap();
    let mut effects = 0;
    assert!(matches!(
        owner.dispatch(
            &mut turn,
            first.close.unwrap().into_command().unwrap(),
            |_| {
                effects += 1;
                Ok::<_, ()>(())
            }
        ),
        DispatchReport::Dispatched
    ));
    let original = ObservationIdentity {
        stream: positive(StreamId::new(2)),
        stamp: ReceiveStamp {
            unix_ns: 701,
            monotonic_ns: 701,
        },
        ..qa_original_identity(ObservationClass::Gap)
    };
    let gap = p2_queued_gap(&handle, &mut turn, original);
    assert_eq!(gap.cut_side(), domain::capture_session::CutSide::PostCut);
    assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
    let second_failure = TerminalFailure {
        stream: original.stream,
        connection: positive(ConnectionId::new(2)),
        observed_tag: original.tag.unwrap(),
        current_epoch: original.epoch,
        context: active(),
        stamp: ReceiveStamp {
            unix_ns: 702,
            monotonic_ns: 702,
        },
        input_class: InputClass::Raw,
        attempt: AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2))),
        cause: FailureCause::QueueOverflow,
    };
    if terminate_gap_scope {
        let second = handle.terminate(&mut turn, second_failure).unwrap();
        assert!(second.first);
        assert!(matches!(
            owner.dispatch(
                &mut turn,
                second.close.unwrap().into_command().unwrap(),
                |_| {
                    effects += 1;
                    Ok::<_, ()>(())
                }
            ),
            DispatchReport::Dispatched
        ));
        assert_eq!(
            handle.authority().terminal_failure(original.stream),
            Some(second_failure)
        );
    }
    let before = owner.session_status();
    let ledger = handle.authority().ownership_report();
    let prefix = handle.prefix();
    let calls = owner.sink_persist_calls();
    let bytes = fs::read(&wal.0).unwrap();
    let eligible = handle.gap_extension_eligible(&turn, &gap);
    let extension = handle.extend_gap_observation(&mut turn, &gap, p2_expanded_gap(original, 2));
    let rejected = matches!(extension, Err(AuthorityError::InvalidOwner));
    eprintln!(
        "INDEPENDENT42 terminalGap terminated={terminate_gap_scope} eligibility={eligible:?} extension={extension:?} candidate={:?}",
        handle.authority().terminal_failure(original.stream)
    );
    assert_eq!(owner.session_status(), before);
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.sink_persist_calls(), calls);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    qa_timer_failure_marker(&handle, &mut turn, &mut sink, RecordingGate::Durable);
    gap.set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    let written_identity = if rejected {
        original
    } else {
        p2_expanded_gap(original, 2)
    };
    let actual = qa_gap(
        handle.prefix().next_record.get(),
        written_identity,
        Reason::QueueOverflow,
    );
    sink.persist_owned(&mut turn, &actual, RecordingGate::Durable, &gap)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &gap);
    drop(gap);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    let (records, physical) = read_all(&wal.0);
    assert_eq!(records.last(), Some(&actual));
    assert_eq!(physical.status, ArchiveStatus::ValidPrefixIncomplete);
    assert!(
        !records
            .iter()
            .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
    );
    assert!(owner.session_status().failed);
    eprintln!(
        "INDEPENDENT42 terminalGap physical={:?} written={actual:?} W=0 effects={effects} seals=0",
        physical.status
    );
    if terminate_gap_scope {
        assert_eq!(
            eligible,
            Ok(false),
            "terminated scope cannot extend admission of a new loss"
        );
        assert!(
            rejected,
            "original terminal Candidate remains diagnostic only, never new GAP accounting"
        );
    } else {
        assert_eq!(eligible, Ok(true));
        assert!(
            !rejected,
            "healthy PostCut neighbor same-tail extension is lawful"
        );
    }
}

#[test]
fn independent_qa_42_durable_terminal_scope_cannot_extend_old_postcut_gap() {
    independent_qa_42_postcut_scope_gap(true);
}

#[test]
fn independent_qa_42_durable_active_postcut_neighbor_gap_extension_control() {
    independent_qa_42_postcut_scope_gap(false);
}

#[test]
fn independent_qa_42_durable_finalized_terminal_call_preserves_final_immutable_reports() {
    let wal = TempWal::new("independent-42-finalized-terminal");
    let (mut owner, mut turn, handle, mut sink) =
        owner_with_fault(&wal.0, RecordingGate::Durable, None);
    let identity = qa_original_identity(ObservationClass::Raw);
    let work = qa_admit(&handle, &mut turn, identity);
    let original = qa_raw(5, identity, false);
    sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &work)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &work);
    drop(work);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("genuine healthy original finalizes first");
    };
    owner.finalize(&mut turn, &mut proof).unwrap();
    let (records, physical) = read_all(&wal.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    assert_eq!(records.len(), 7);
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
    let status = owner.session_status();
    let ledger = handle.authority().ownership_report();
    let unsettled = handle.authority().unsettled_summary();
    let closes = owner.outstanding_close_owners();
    let prefix = handle.prefix();
    let watermarks = owner.watermarks();
    let calls = owner.sink_persist_calls();
    let bytes = fs::read(&wal.0).unwrap();
    assert_eq!(status.lifecycle, SessionLifecycle::Finalized);
    assert!(!status.failed);
    assert!(matches!(
        handle.mandatory_close(&mut turn, identity.stream, identity.epoch, None),
        Err(AuthorityError::SessionClosed)
    ));
    assert_eq!(owner.session_status(), status);
    let mut next_received_failure = terminal();
    next_received_failure.attempt = AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2)));
    let outcome = handle.terminate(&mut turn, next_received_failure);
    eprintln!(
        "INDEPENDENT42 Finalized terminate={outcome:?}; before={status:?}; after={:?}; physical=Complete records7 seals1+1",
        owner.session_status()
    );
    assert!(matches!(outcome, Err(AuthorityError::SessionClosed)));
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(owner.outstanding_close_owners(), closes);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(owner.sink_persist_calls(), calls);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes);
    assert_eq!(read_all(&wal.0), (records, physical));
    assert_eq!(
        owner.session_status(),
        status,
        "Finalized exposes only immutable final reports; rejected late terminal call changes none"
    );
    assert_eq!(handle.authority().unsettled_summary(), unsettled);
}

// Fresh independent QA: public owner-minted route and actual Durable backend.
// Preparing H1 is not archive-record admission. Its first activated stage is.
fn independent_qa_42_generated_stage_gap_barrier(activate_first: bool) {
    let wal = TempWal::new("independent-42-generated-gap-barrier");
    let (mut owner, mut turn, handle, mut sink) =
        owner_two_scopes_with_gate(&wal.0, RecordingGate::Durable);
    let down_identity = qa_original_identity(ObservationClass::Disconnected);
    let plan = qa_admit(&handle, &mut turn, down_identity);
    let down = qa_transport(7, down_identity, Transport::Down);
    sink.persist_owned(&mut turn, &down, RecordingGate::Durable, &plan)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &plan);
    let close = qa_pending_down_close(&owner, down_identity, &plan);
    let command = leased(owner.reclaim_close(&mut turn, close))
        .into_command()
        .unwrap();
    let mut close_effects = 0;
    assert!(matches!(
        owner.dispatch(&mut turn, command, |_| {
            close_effects += 1;
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(close_effects, 1);
    plan.set_kind(&mut turn, WorkKind::PendingPlan).unwrap();

    let gap_identity = ObservationIdentity {
        stream: positive(StreamId::new(2)),
        stamp: ReceiveStamp {
            unix_ns: 6,
            monotonic_ns: 6,
        },
        ..qa_original_identity(ObservationClass::Gap)
    };
    let gap = p2_queued_gap(&handle, &mut turn, gap_identity);
    let before_prepare = handle.authority().ownership_report();
    let prefix_before_prepare = handle.prefix();
    let calls_before_prepare = owner.sink_persist_calls();
    let bytes_before_prepare = fs::read(&wal.0).unwrap();
    assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
    handle.retain_generated_plan(&mut turn, &plan).unwrap();
    assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
    let mut prepared_ledger = before_prepare;
    prepared_ledger.pending_observations += 1;
    assert_eq!(handle.authority().ownership_report(), prepared_ledger);
    assert_eq!(handle.prefix(), prefix_before_prepare);
    assert_eq!(owner.sink_persist_calls(), calls_before_prepare);
    assert_eq!(fs::read(&wal.0).unwrap(), bytes_before_prepare);
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, &plan, None),
        Err(AuthorityError::NotQuiescent)
    );

    let binding = binding();
    let changes = [
        EpochChange::Connection {
            owner: binding.connection_id,
            expected: binding.tag.connection,
            next: binding.tag.connection.checked_next().unwrap(),
        },
        EpochChange::Subscription {
            owner: binding.id,
            expected: binding.tag.subscription,
            next: binding.tag.subscription.checked_next().unwrap(),
        },
        EpochChange::Book {
            owner: binding.book_id.unwrap(),
            expected: binding.tag.book.unwrap(),
            next: binding.tag.book.unwrap().checked_next().unwrap(),
        },
    ];
    let mut expected = vec![down];
    if activate_first {
        let connection = qa_epoch(8, down_identity, changes[0].clone());
        sink.persist_owned(&mut turn, &connection, RecordingGate::Durable, &plan)
            .unwrap();
        expected.push(connection);
        assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
        let status = owner.session_status();
        let ledger = handle.authority().ownership_report();
        let unsettled = handle.authority().unsettled_summary();
        let closes = owner.outstanding_close_owners();
        let prefix = handle.prefix();
        let watermarks = owner.watermarks();
        let bytes = fs::read(&wal.0).unwrap();
        let calls = owner.sink_persist_calls();
        let id = gap.id();
        let cut = gap.cut_side();
        for _ in 0..100 {
            assert_eq!(
                handle.extend_gap_observation(&mut turn, &gap, p2_expanded_gap(gap_identity, 2)),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(owner.session_status(), status);
            assert_eq!(handle.authority().ownership_report(), ledger);
            assert_eq!(handle.authority().unsettled_summary(), unsettled);
            assert_eq!(owner.outstanding_close_owners(), closes);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.watermarks(), watermarks);
            assert_eq!(owner.sink_persist_calls(), calls);
            assert_eq!(gap.id(), id);
            assert_eq!(gap.cut_side(), cut);
            assert_eq!(fs::read(&wal.0).unwrap(), bytes);
        }
        // Concrete matching expanded bytes are denied too; original stays retryable.
        p2_write_original_gap_after_rejection(
            (&wal, &owner, &handle),
            &mut turn,
            &mut sink,
            &gap,
            gap_identity,
        );
        expected.push(qa_gap(9, gap_identity, Reason::QueueOverflow));
    } else {
        let ledger = handle.authority().ownership_report();
        let id = gap.id();
        let cut = gap.cut_side();
        handle
            .extend_gap_observation(&mut turn, &gap, p2_expanded_gap(gap_identity, 2))
            .unwrap();
        assert_eq!(handle.authority().ownership_report(), ledger);
        assert_eq!(gap.id(), id);
        assert_eq!(gap.cut_side(), cut);
        gap.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let expanded = qa_gap(8, p2_expanded_gap(gap_identity, 2), Reason::QueueOverflow);
        sink.persist_owned(&mut turn, &expanded, RecordingGate::Durable, &gap)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &gap);
        expected.push(expanded);
    }
    drop(gap);
    let first_remaining = usize::from(activate_first);
    for change in changes.into_iter().skip(first_remaining) {
        let stage = qa_epoch(handle.prefix().next_record.get(), down_identity, change);
        sink.persist_owned(&mut turn, &stage, RecordingGate::Durable, &plan)
            .unwrap();
        expected.push(stage);
    }
    qa_settle_once(&handle, &mut turn, &sink, &plan);
    drop(plan);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(owner.outstanding_close_owners().iter().count(), 0);
    let (before_seals, incomplete) = read_all(&wal.0);
    assert_eq!(&before_seals[6..], expected.as_slice());
    assert_eq!(incomplete.status, ArchiveStatus::ValidPrefixIncomplete);
    let ticket = owner.begin_finalization(&mut turn).unwrap();
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("original Down/Close, truthful GAP and three fresh H1 stages complete");
    };
    owner.finalize(&mut turn, &mut proof).unwrap();
    let (records, physical) = read_all(&wal.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    assert_eq!(physical.input_quality, Some(InputQuality::Unknown));
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    assert_eq!(records.len(), 13);
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
    eprintln!(
        "INDEPENDENT42 generated activation={activate_first} genuine Durable Complete records={} close_effects={close_effects} W=0 seals=1+1",
        records.len()
    );
}

#[test]
fn independent_qa_42_durable_gap_rejects_other_scope_activated_generated_stage_preservingly() {
    independent_qa_42_generated_stage_gap_barrier(true);
}

#[test]
fn independent_qa_42_durable_gap_prepared_unactivated_generated_plan_allows_lawful_extension() {
    independent_qa_42_generated_stage_gap_barrier(false);
}

// Public reports only: private scheduler/ordinal arithmetic is covered in domain.
macro_rules! qa42_reports {
    ($owner:expr, $handle:expr, $wal:expr) => {{
        let authority = $handle.authority();
        (
            $owner.session_status(),
            authority.ownership_report(),
            authority.unsettled_summary(),
            $owner.outstanding_close_owners(),
            (
                authority.disposition(),
                authority.scope_disposition(positive(StreamId::new(1))),
                authority.scope_disposition(positive(StreamId::new(2))),
                authority.terminal_failure(positive(StreamId::new(1))),
                authority.terminal_failure(positive(StreamId::new(2))),
                authority.archive_failure_observation(),
                authority.scopes(),
            ),
            (
                $handle.prefix(),
                $owner.watermarks(),
                [
                    authority.trusted_watermark(WatermarkKind::Accepted),
                    authority.trusted_watermark(WatermarkKind::Appended),
                    authority.trusted_watermark(WatermarkKind::Written),
                    authority.trusted_watermark(WatermarkKind::Flushed),
                    authority.trusted_watermark(WatermarkKind::Durable),
                ],
            ),
            $owner.sink_persist_calls(),
            fs::read(&$wal.0).unwrap(),
            read_all(&$wal.0),
        )
    }};
}

fn qa42_original_failure_marker(
    handle: &SupervisorSessionHandle,
    turn: &mut SessionTurn,
    sink: &mut BoundRecordSink,
    gate: RecordingGate,
) {
    let descriptor = handle.archive_failure_observation().unwrap();
    let marker = frame(
        handle.prefix().next_record.get(),
        Record::Control(ControlRecord {
            context: descriptor.context,
            value: Control::Recording(RecordingEvidence {
                health: RecordingHealth::Failed,
                kind: descriptor.kind,
                through: handle.trusted_watermark(descriptor.kind),
                reason: descriptor.reason,
            }),
        }),
    );
    sink.persist_marker(turn, &marker, gate).unwrap();
    handle
        .authority()
        .marker_confirmed(turn, marker.record_no)
        .unwrap();
    assert_eq!(handle.archive_failure_observation(), Some(descriptor));
}

fn qa42_dispatch_terminal(
    owner: &mut CaptureSessionOwner,
    turn: &mut SessionTurn,
    handle: &SupervisorSessionHandle,
    failure: TerminalFailure,
) -> CloseOwnerRef {
    let report = handle.terminate(turn, failure).unwrap();
    assert!(report.first);
    let close = report.close_owner;
    let mut effects = 0;
    assert!(matches!(
        owner.dispatch(turn, report.close.unwrap().into_command().unwrap(), |_| {
            effects += 1;
            Ok::<_, ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(effects, 1, "only the original lawful failure Close effect");
    close
}

#[test]
fn qa42_durable_finalized_terminal_hardstop_and_sync_repeats_preserve_all_public_reports() {
    for with_original_close in [false, true] {
        let wal = TempWal::new("qa42-finalized-terminal-routes");
        let foreign_wal = TempWal::new("qa42-finalized-terminal-foreign");
        let (mut owner, mut turn, handle, mut sink) =
            owner_with_fault(&wal.0, RecordingGate::Durable, None);
        let (foreign_owner, mut foreign_turn, foreign_handle, _foreign_sink) =
            owner_with_fault(&foreign_wal.0, RecordingGate::Durable, None);
        let identity = qa_original_identity(if with_original_close {
            ObservationClass::Disconnected
        } else {
            ObservationClass::Raw
        });
        let work = qa_admit(&handle, &mut turn, identity);
        let original = if with_original_close {
            qa_transport(5, identity, Transport::Down)
        } else {
            qa_raw(5, identity, false)
        };
        sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &work)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &work);
        let original_close = with_original_close.then(|| {
            let close = qa_pending_down_close(&owner, identity, &work);
            let command = leased(owner.reclaim_close(&mut turn, close.clone()))
                .into_command()
                .unwrap();
            assert!(matches!(
                owner.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
                DispatchReport::Dispatched
            ));
            close
        });
        drop(work);
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("all original Durable obligations and Close settled first");
        };
        owner.finalize(&mut turn, &mut proof).unwrap();
        let (records, physical) = read_all(&wal.0);
        assert_eq!(physical.status, ArchiveStatus::Complete);
        assert_eq!(records.len(), 7);
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.value, Record::SegmentSeal(_)))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.value, Record::ArchiveSeal(_)))
                .count(),
            1
        );
        assert_eq!(
            owner.session_status().lifecycle,
            SessionLifecycle::Finalized
        );
        assert!(!owner.session_status().failed);
        let before = qa42_reports!(owner, handle, wal);
        let foreign_before = qa42_reports!(foreign_owner, foreign_handle, foreign_wal);
        let mut failure = terminal();
        failure.attempt = AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2)));
        let late_error = PersistError::typed(PersistErrorKind::Io, "late finalized storage stop");
        for _ in 0..100 {
            assert!(matches!(
                handle.terminate(&mut turn, failure),
                Err(AuthorityError::SessionClosed)
            ));
            assert_eq!(qa42_reports!(owner, handle, wal), before);
            assert!(matches!(
                handle.authority().terminate(&mut turn, failure),
                Err(AuthorityError::SessionClosed)
            ));
            assert_eq!(qa42_reports!(owner, handle, wal), before);
            assert_eq!(
                handle.authority().hard_stop(&mut turn, late_error),
                Err(AuthorityError::SessionClosed)
            );
            assert_eq!(qa42_reports!(owner, handle, wal), before);
            assert_eq!(
                handle.authority().storage_stopped(&mut turn, late_error),
                Err(AuthorityError::SessionClosed)
            );
            assert_eq!(qa42_reports!(owner, handle, wal), before);
            assert_eq!(
                handle.authority().synchronize_obligations(&mut turn),
                Ok(())
            );
            assert_eq!(qa42_reports!(owner, handle, wal), before);
            if let Some(close) = &original_close {
                assert_eq!(
                    handle
                        .authority()
                        .close_state(identity.stream, identity.epoch),
                    Ok(CloseState::Settled)
                );
                assert!(matches!(
                    owner.reclaim_close(&mut turn, close.clone()),
                    CloseLeaseReport::Rejected(AuthorityError::SessionClosed)
                ));
                assert_eq!(qa42_reports!(owner, handle, wal), before);
            }
        }
        assert!(matches!(
            handle.terminate(&mut foreign_turn, failure),
            Err(AuthorityError::AuthorityMismatch)
        ));
        assert!(matches!(
            handle.authority().terminate(&mut foreign_turn, failure),
            Err(AuthorityError::AuthorityMismatch)
        ));
        assert_eq!(
            handle.authority().hard_stop(&mut foreign_turn, late_error),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert_eq!(
            handle
                .authority()
                .storage_stopped(&mut foreign_turn, late_error),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert_eq!(
            handle
                .authority()
                .synchronize_obligations(&mut foreign_turn),
            Err(AuthorityError::AuthorityMismatch)
        );
        let unknown_binding = TerminalFailure {
            connection: positive(ConnectionId::new(2)),
            ..failure
        };
        assert!(matches!(
            handle.terminate(&mut turn, unknown_binding),
            Err(AuthorityError::InvalidBinding)
        ));
        assert!(matches!(
            handle.authority().terminate(&mut turn, unknown_binding),
            Err(AuthorityError::InvalidBinding)
        ));
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::TicketConsumed
        ));
        assert!(matches!(
            owner.finalize(&mut turn, &mut proof),
            Err(OwnerError::Authority(AuthorityError::ProofConsumed))
        ));
        assert_eq!(qa42_reports!(owner, handle, wal), before);
        assert_eq!(
            qa42_reports!(foreign_owner, foreign_handle, foreign_wal),
            foreign_before
        );
    }
}

#[test]
fn qa42_durable_closing_failure_before_and_after_ready_revokes_proof_and_keeps_original_close() {
    for proof_issued in [false, true] {
        for storage_stop in [false, true] {
            let wal = TempWal::new("qa42-closing-terminal-boundary");
            let (mut owner, mut turn, handle, mut sink) =
                owner_with_fault(&wal.0, RecordingGate::Durable, None);
            let identity = qa_original_identity(ObservationClass::Disconnected);
            let work = qa_admit(&handle, &mut turn, identity);
            let down = qa_transport(5, identity, Transport::Down);
            sink.persist_owned(&mut turn, &down, RecordingGate::Durable, &work)
                .unwrap();
            qa_settle_once(&handle, &mut turn, &sink, &work);
            let close = qa_pending_down_close(&owner, identity, &work);
            drop(work);
            if proof_issued {
                let command = leased(owner.reclaim_close(&mut turn, close.clone()))
                    .into_command()
                    .unwrap();
                assert!(matches!(
                    owner.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
                    DispatchReport::Dispatched
                ));
            }
            let ticket = owner.begin_finalization(&mut turn).unwrap();
            let mut proof = if proof_issued {
                let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
                    panic!("genuine Ready before failure");
                };
                Some(proof)
            } else {
                assert!(matches!(
                    handle.quiesce(&mut turn, &ticket),
                    QuiescenceReport::NotReady(_)
                ));
                None
            };
            let before_prefix = handle.prefix();
            let before_bytes = fs::read(&wal.0).unwrap();
            let before_calls = owner.sink_persist_calls();
            if storage_stop {
                handle
                    .authority()
                    .hard_stop(
                        &mut turn,
                        PersistError::typed(PersistErrorKind::Io, "lawful Closing hard stop"),
                    )
                    .unwrap();
            } else {
                let termination = handle.terminate(&mut turn, terminal()).unwrap();
                assert!(termination.first);
                assert_eq!(termination.close_owner, close);
                drop(termination.close);
            }
            assert_eq!(
                owner.session_status().lifecycle,
                SessionLifecycle::DiagnosticClosing
            );
            assert!(owner.session_status().failed);
            assert_eq!(handle.prefix(), before_prefix);
            assert_eq!(owner.sink_persist_calls(), before_calls);
            assert_eq!(fs::read(&wal.0).unwrap(), before_bytes);
            assert_eq!(
                handle.authority().ensure_admission_open(&turn),
                if storage_stop {
                    Err(AuthorityError::StorageStopped)
                } else {
                    Err(AuthorityError::SessionClosing)
                }
            );
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
            ));
            if let Some(proof) = &mut proof {
                assert!(matches!(
                    owner.finalize(&mut turn, proof),
                    Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
                ));
            }
            if !proof_issued {
                let ledger = handle.authority().ownership_report();
                drop(leased(owner.reclaim_close(&mut turn, close.clone())));
                assert_eq!(handle.authority().ownership_report(), ledger);
                let command = leased(owner.reclaim_close(&mut turn, close.clone()))
                    .into_command()
                    .unwrap();
                assert!(matches!(
                    owner.dispatch(&mut turn, command, |_| Err::<(), _>(
                        "possible original Close effect"
                    )),
                    DispatchReport::DispatchFailed {
                        effect: AmbiguousEffect::Unknown,
                        ..
                    }
                ));
                let command = leased(owner.reclaim_close(&mut turn, close.clone()))
                    .into_command()
                    .unwrap();
                assert!(matches!(
                    owner.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
                    DispatchReport::Dispatched
                ));
            }
            assert_eq!(
                handle
                    .authority()
                    .close_state(identity.stream, identity.epoch),
                Ok(CloseState::Settled)
            );
            if !storage_stop {
                qa42_original_failure_marker(&handle, &mut turn, &mut sink, RecordingGate::Durable);
            }
            assert!(matches!(
                owner.close_diagnostic(&mut turn).outcome,
                Ok(DiagnosticCloseState::Closed)
            ));
            assert!(matches!(
                owner.reclaim_close(&mut turn, close),
                CloseLeaseReport::AlreadySettled
            ));
            let (records, physical) = read_all(&wal.0);
            assert_eq!(physical.status, ArchiveStatus::ValidPrefixIncomplete);
            assert!(
                !records
                    .iter()
                    .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
            );
            assert_eq!(records[4], down);
        }
    }
}

#[test]
fn qa42_durable_failed_scope_gap_at_three_old_stages_rejects_extension_and_drains_once() {
    for cap in [5, 9] {
        for confirmed_stage in 0..=2 {
            let wal = TempWal::new("qa42-failed-original-gap-stages");
            let (mut owner, mut turn, handle, mut sink) = if cap == 5 {
                owner_with_fault(&wal.0, RecordingGate::Durable, None)
            } else {
                owner_two_scopes_with_gate(&wal.0, RecordingGate::Durable)
            };
            let first_close = (cap == 9)
                .then(|| qa42_dispatch_terminal(&mut owner, &mut turn, &handle, terminal()));
            let identity = ObservationIdentity {
                stream: positive(StreamId::new(if cap == 5 { 1 } else { 2 })),
                stamp: ReceiveStamp {
                    unix_ns: 701,
                    monotonic_ns: 701,
                },
                ..qa_original_identity(ObservationClass::Gap)
            };
            let gap = p2_queued_gap(&handle, &mut turn, identity);
            assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
            if cap == 9 {
                assert_eq!(gap.cut_side(), domain::capture_session::CutSide::PostCut);
                qa42_original_failure_marker(&handle, &mut turn, &mut sink, RecordingGate::Durable);
            }
            let original = qa_gap(
                handle.prefix().next_record.get(),
                identity,
                Reason::QueueOverflow,
            );
            if confirmed_stage != 0 {
                gap.set_kind(&mut turn, WorkKind::InFlightObservation)
                    .unwrap();
            }
            if confirmed_stage == 2 {
                sink.persist_owned(&mut turn, &original, RecordingGate::Durable, &gap)
                    .unwrap();
            }
            let failure = TerminalFailure {
                stream: identity.stream,
                connection: positive(ConnectionId::new(if cap == 5 { 1 } else { 2 })),
                current_epoch: identity.epoch,
                observed_tag: identity.tag.unwrap(),
                stamp: ReceiveStamp {
                    unix_ns: 702,
                    monotonic_ns: 702,
                },
                attempt: AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2))),
                ..terminal()
            };
            let close = qa42_dispatch_terminal(&mut owner, &mut turn, &handle, failure);
            let before = qa42_reports!(owner, handle, wal);
            let id = gap.id();
            let cut = gap.cut_side();
            for _ in 0..100 {
                assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
                assert_eq!(
                    handle.extend_gap_observation(&mut turn, &gap, p2_expanded_gap(identity, 2)),
                    Err(AuthorityError::InvalidOwner)
                );
                let repeat = handle.terminate(&mut turn, failure).unwrap();
                assert!(!repeat.first);
                assert!(repeat.close.is_none());
                assert_eq!(repeat.close_owner, close);
                assert_eq!(gap.id(), id);
                assert_eq!(gap.cut_side(), cut);
                assert_eq!(qa42_reports!(owner, handle, wal), before);
            }
            if confirmed_stage != 2 {
                p2_write_original_gap_after_rejection(
                    (&wal, &owner, &handle),
                    &mut turn,
                    &mut sink,
                    &gap,
                    identity,
                );
            } else {
                qa_settle_once(&handle, &mut turn, &sink, &gap);
            }
            drop(gap);
            if cap == 5 {
                qa42_original_failure_marker(&handle, &mut turn, &mut sink, RecordingGate::Durable);
            }
            assert_eq!(
                handle.authority().terminal_failure(identity.stream),
                Some(failure)
            );
            assert_eq!(handle.authority().ownership_report().work_used, 0);
            assert!(matches!(
                owner.close_diagnostic(&mut turn).outcome,
                Ok(DiagnosticCloseState::Closed)
            ));
            let (records, physical) = read_all(&wal.0);
            assert_eq!(physical.status, ArchiveStatus::ValidPrefixIncomplete);
            assert_eq!(records.iter().filter(|f| f == &&original).count(), 1);
            assert!(
                !records
                    .iter()
                    .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
            );
            assert_eq!(
                handle
                    .authority()
                    .terminal_failure(identity.stream)
                    .unwrap()
                    .attempt,
                AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2)))
            );
            drop(close);
            drop(first_close);
        }
    }
}

#[test]
fn qa42_durable_healthy_postcut_gap_thirty_two_extensions_keep_original_owner_and_cut() {
    let wal = TempWal::new("qa42-healthy-postcut-gap32");
    let (mut owner, mut turn, handle, mut sink) =
        owner_two_scopes_with_gate(&wal.0, RecordingGate::Durable);
    let _first_close = qa42_dispatch_terminal(&mut owner, &mut turn, &handle, terminal());
    let identity = ObservationIdentity {
        stream: positive(StreamId::new(2)),
        stamp: ReceiveStamp {
            unix_ns: 701,
            monotonic_ns: 701,
        },
        ..qa_original_identity(ObservationClass::Gap)
    };
    let gap = p2_queued_gap(&handle, &mut turn, identity);
    let reserved = handle.reserve_work(&mut turn, WorkKind::Result).unwrap();
    let before = qa42_reports!(owner, handle, wal);
    let id = gap.id();
    let cut = gap.cut_side();
    for last in 2..=33 {
        assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
        handle
            .extend_gap_observation(&mut turn, &gap, p2_expanded_gap(identity, last))
            .unwrap();
        assert_eq!(gap.id(), id);
        assert_eq!(gap.cut_side(), cut);
        assert_eq!(qa42_reports!(owner, handle, wal), before);
    }
    assert!(
        handle
            .authority()
            .terminal_failure(identity.stream)
            .is_none()
    );
    qa42_original_failure_marker(&handle, &mut turn, &mut sink, RecordingGate::Durable);
    gap.set_kind(&mut turn, WorkKind::InFlightObservation)
        .unwrap();
    let expanded = qa_gap(
        handle.prefix().next_record.get(),
        p2_expanded_gap(identity, 33),
        Reason::QueueOverflow,
    );
    sink.persist_owned(&mut turn, &expanded, RecordingGate::Durable, &gap)
        .unwrap();
    qa_settle_once(&handle, &mut turn, &sink, &gap);
    drop(gap);
    drop(reserved);
    assert_eq!(handle.authority().ownership_report().work_used, 0);
    assert_eq!(read_all(&wal.0).0.last(), Some(&expanded));
    assert!(owner.session_status().failed);
    assert!(
        handle
            .authority()
            .terminal_failure(identity.stream)
            .is_none()
    );
    assert_eq!(cut, domain::capture_session::CutSide::PostCut);
}

fn qa42_failed_scope_requested_allocation(gate: RecordingGate) {
    for cap in [5, 9] {
        let wal = TempWal::new("qa42-failed-gap-allocation");
        let probe = AllocationProbe::begin();
        let (mut owner, mut turn, handle, mut sink) = if cap == 5 {
            owner_with_fault(&wal.0, gate, None)
        } else {
            owner_two_scopes_with_gate(&wal.0, gate)
        };
        let memory = owner.memory_report();
        let metadata = handle.authority().ownership_report();
        let ceiling = metadata.metadata_ceiling_bytes
            + memory.known_metadata_backing_bytes
            + memory.registry_metadata_bound
            + memory.encoder_workspace_bound
            + MAX_CAPTURE_PATH_BYTES
            + 8192;
        let first_close =
            (cap == 9).then(|| qa42_dispatch_terminal(&mut owner, &mut turn, &handle, terminal()));
        let identity = ObservationIdentity {
            stream: positive(StreamId::new(if cap == 5 { 1 } else { 2 })),
            stamp: ReceiveStamp {
                unix_ns: 701,
                monotonic_ns: 701,
            },
            ..qa_original_identity(ObservationClass::Gap)
        };
        let gap = p2_queued_gap(&handle, &mut turn, identity);
        let mut reserved = Vec::new();
        while handle.authority().ownership_report().work_used < metadata.work_limit {
            reserved.push(handle.reserve_work(&mut turn, WorkKind::Result).unwrap());
        }
        let failure = TerminalFailure {
            stream: identity.stream,
            connection: positive(ConnectionId::new(if cap == 5 { 1 } else { 2 })),
            observed_tag: identity.tag.unwrap(),
            current_epoch: identity.epoch,
            stamp: ReceiveStamp {
                unix_ns: 702,
                monotonic_ns: 702,
            },
            attempt: AttemptIdentity::Candidate(positive(CaptureAttemptNo::new(2))),
            ..terminal()
        };
        let close = qa42_dispatch_terminal(&mut owner, &mut turn, &handle, failure);
        let status = owner.session_status();
        let ownership = handle.authority().ownership_report();
        let unsettled = handle.authority().unsettled_summary();
        let closes = owner.outstanding_close_owners();
        let prefix = handle.prefix();
        let watermarks = owner.watermarks();
        let calls = owner.sink_persist_calls();
        let cut = gap.cut_side();
        let id = gap.id();
        let baseline = probe.sample().live_requested_bytes;
        for _ in 0..100 {
            assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
            assert_eq!(
                handle.extend_gap_observation(&mut turn, &gap, p2_expanded_gap(identity, 2)),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(owner.session_status(), status);
            assert_eq!(handle.authority().ownership_report(), ownership);
            assert_eq!(handle.authority().unsettled_summary(), unsettled);
            assert_eq!(owner.outstanding_close_owners(), closes);
            assert_eq!(handle.prefix(), prefix);
            assert_eq!(owner.watermarks(), watermarks);
            assert_eq!(owner.sink_persist_calls(), calls);
            assert_eq!(gap.id(), id);
            assert_eq!(gap.cut_side(), cut);
            assert_eq!(probe.sample().live_requested_bytes, baseline);
        }
        if cap == 9 {
            qa42_original_failure_marker(&handle, &mut turn, &mut sink, gate);
        }
        gap.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let original = qa_gap(
            handle.prefix().next_record.get(),
            identity,
            Reason::QueueOverflow,
        );
        sink.persist_owned(&mut turn, &original, gate, &gap)
            .unwrap();
        qa_settle_once(&handle, &mut turn, &sink, &gap);
        drop(gap);
        if cap == 5 {
            qa42_original_failure_marker(&handle, &mut turn, &mut sink, gate);
        }
        drop(reserved);
        assert_eq!(handle.authority().ownership_report().work_used, 0);
        let measured = probe.sample();
        assert!(!measured.unmatched_deallocation);
        assert!(measured.peak_requested_bytes <= ceiling);
        drop(original);
        drop(closes);
        drop(close);
        drop(first_close);
        drop(sink);
        drop(handle);
        drop(turn);
        drop(owner);
        assert_eq!(probe.sample().live_requested_bytes, 0);
        assert!(!probe.sample().unmatched_deallocation);
        drop(probe);
        eprintln!(
            "qa42 terminal GAP allocation gate={gate:?} cap={cap} N={} rejects=100 baseline={baseline} peak={} ceiling={ceiling} teardown_live=0",
            if cap == 5 { 1 } else { 2 },
            measured.peak_requested_bytes
        );
    }
}

#[test]
fn qa42_written_failed_scope_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    qa42_failed_scope_requested_allocation(RecordingGate::Written);
}

#[test]
fn qa42_flushed_failed_scope_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    qa42_failed_scope_requested_allocation(RecordingGate::Flushed);
}

#[test]
fn qa42_durable_failed_scope_gap_rejection_requested_allocation_cap5_cap9_repeats_teardown() {
    qa42_failed_scope_requested_allocation(RecordingGate::Durable);
}
