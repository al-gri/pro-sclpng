use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use domain::artifact::ArtifactRef;
use domain::capture_session::{
    AmbiguousEffect, AttemptIdentity, AuthorityError, BoundRecordSink, CloseLease,
    CloseLeaseReport, CloseOwnerRef, CloseState, CloseStorage, CommandKind, DispatchReport,
    FailureCause, InputClass, MarkerState, PersistBoundaryError, PersistError, PersistErrorKind,
    QuiescenceReport, ReceiveStamp, RetentionBudget, ScopeBinding, SessionLifecycle, SessionTurn,
    SupervisorSessionHandle, TerminalFailure, WorkKind,
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
    let mut definitions = bootstrap();
    let mut config = definitions.pop().expect("config");
    config.record_no = record(6);
    let Record::ConfigDefinition(config_definition) = &mut config.value else {
        unreachable!("fixture config")
    };
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
    let proof = match handle.quiesce(&mut turn, &ticket) {
        QuiescenceReport::Ready(proof) => proof,
        other => panic!("settled owner must quiesce: {other:?}"),
    };
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    let finalized = owner.finalize(&mut turn, proof).expect("canonical seals");
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
    let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("same ticket must become ready after explicit settlement")
    };
    owner
        .finalize(&mut turn, proof)
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
    let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
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
        owner.finalize(&mut turn, proof),
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
    let QuiescenceReport::Ready(proof) = handle_a.quiesce(&mut turn_a, &ticket_a) else {
        panic!("foreign errors must preserve rightful issuance")
    };
    owner_a
        .finalize(&mut turn_a, proof)
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
    let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("same ticket must become ready after complete Down settlement")
    };
    owner
        .finalize(&mut turn, proof)
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
        let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("settled healthy session")
        };
        handle
            .authority()
            .consume_proof(&mut turn, proof)
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
