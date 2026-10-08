use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use domain::artifact::ArtifactRef;
use domain::capture_session::{
    AmbiguousEffect, AttemptIdentity, AuthorityError, BoundRecordSink, CloseLease,
    CloseLeaseReport, CloseOwnerRef, CloseState, CloseStorage, CommandKind, DispatchReport,
    FailureCause, InputClass, MarkerState, ObservationClass, ObservationIdentity,
    PersistBoundaryError, PersistError, PersistErrorKind, QuiescenceReport, ReceiveStamp,
    RetentionBudget, ScopeBinding, SessionLifecycle, SessionTurn, SupervisorSessionHandle,
    TerminalFailure, WorkKind, WorkOwner,
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
        owner.dispatch(&mut turn, lease.into_command(), |command| {
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
        owner.dispatch(&mut turn, retry.into_command(), |_| Err::<(), _>(
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
        owner.dispatch(&mut turn, retry.into_command(), |_| Ok::<_, ()>(())),
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
    let command = termination.close.expect("initial close").into_command();
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
        owner.dispatch(&mut turn, reclaimed.into_command(), |_| Ok::<_, ()>(())),
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
            owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
    assert!(
        matches!(
            sink.persist_owned(turn, submitted, gate, work),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding | AuthorityError::InvalidOwner
            ))
        ),
        "metadata/stage substitution must reject before backend write: {submitted:?}"
    );
    assert_eq!(handle.authority().ownership_report(), ledger);
    assert_eq!(owner.session_status(), status);
    assert_eq!(handle.prefix(), prefix);
    assert_eq!(owner.outstanding_close_owners(), close);
    assert_eq!(owner.watermarks(), watermarks);
    assert_eq!(fs::read(&wal.0).unwrap(), physical);
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
    let work = qa_admit(&handle, &mut turn, identity);
    identity.attempts = Some((first, positive(CaptureAttemptNo::new(3))));
    identity.loss_count = Some(3);
    handle
        .extend_gap_observation(&mut turn, &work, identity)
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
                owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
            owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
    // This authenticates the common Timer observation only. It makes no claim
    // about the unresolved Ping/Timeout active/obsolete effect-plan contract.
    let wal = TempWal::new("qa-new-timer-identity");
    let (owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
    let identity = qa_original_identity(ObservationClass::Timer {
        timer_id: 99,
        deadline_ns: 5,
    });
    let work = qa_admit(&handle, &mut turn, identity);
    let original = frame(
        5,
        Record::Control(ControlRecord {
            context: qa_context(identity),
            value: Control::Timer {
                stream: identity.stream,
                timer_id: 99,
                deadline_ns: 5,
            },
        }),
    );
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
            0 => *timer_id = 100,
            1 => *deadline_ns = 4,
            2 => control.context.unix_ns = LocalUnixNs::new(6),
            3 => control.context.monotonic_ns = MonotonicNs::new(6),
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
    repeated.record_no = record(6);
    qa_reject_preserving(
        (&wal, &owner, &handle),
        &mut turn,
        &mut sink,
        &work,
        gate,
        &repeated,
    );
    qa_settle_once(&handle, &mut turn, &sink, &work);
    assert_eq!(read_all(&wal.0).0.len(), 5);
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
        owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
        owner.dispatch(&mut turn, lease.into_command(), |command| {
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
        lease.into_command(),
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
        owner.dispatch(&mut turn, lease.into_command(), |_| Err::<(), _>(
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
        owner.dispatch(&mut turn, lease.into_command(), |command| {
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
        assert_eq!(report.input_quality, Some(InputQuality::NoKnownLoss));
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
        owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
                    owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
                owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
        let wal = TempWal::new("qa-down-close-unrelated-owner");
        let (mut owner, mut turn, handle, mut sink) = owner_with_fault(&wal.0, gate, None);
        let other_identity = qa_original_identity(if variant == 3 {
            ObservationClass::Timer {
                timer_id: 99,
                deadline_ns: 5,
            }
        } else {
            ObservationClass::Raw
        });
        let other = if variant == 0 {
            handle
                .reserve_work(&mut turn, WorkKind::PendingPlan)
                .unwrap()
        } else {
            qa_admit(&handle, &mut turn, other_identity)
        };
        let mut down_no = 5;
        if variant >= 2 {
            let other_record = if variant == 2 {
                qa_raw(5, other_identity, false)
            } else {
                frame(
                    5,
                    Record::Control(ControlRecord {
                        context: qa_context(other_identity),
                        value: Control::Timer {
                            stream: other_identity.stream,
                            timer_id: 99,
                            deadline_ns: 5,
                        },
                    }),
                )
            };
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
            owner.dispatch(&mut turn, held.into_command(), |_| Ok::<_, ()>(())),
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
            sink.persist_owned(
                &mut turn,
                &qa_raw(down_no + 1, other_identity, false),
                gate,
                &other,
            )
            .unwrap();
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
        assert_eq!(records.len(), (down_no + u64::from(variant == 1)) as usize);
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
            owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
        owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
        let held = if variant == 1 {
            Some(lease)
        } else {
            if variant == 2 {
                assert!(matches!(
                    owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
                owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
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
