use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use domain::artifact::ArtifactRef;
use domain::capture_session as session;
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, PolicyFields, RecordingGate, SilenceRule, WatermarkKind};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;
use market_data::PublicWsSupervisor as BoundSupervisor;
use market_data::*;
use recording::{BoundedCaptureProfile, CaptureSessionOwner};
use recording::{WalReader, WalWriter};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

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

// Read-only DTOs used by the historical assertion grammar. Every command view
// below is obtained from an opaque lease and dispatched through the canonical
// authority before the test receives it. They are not effect permissions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DrainResult {
    records: Vec<RecordNo>,
    commands: Vec<TransportCommand>,
    events: Vec<SupervisorEvent>,
}

#[derive(Clone, Copy, Debug)]
enum FaultPlan {
    None,
    Weak(RecordingGate),
    Epoch {
        fault_at: usize,
        seen: usize,
        fault: EpochPersistenceFault,
    },
    Timer(EpochPersistenceFault),
}

#[derive(Debug)]
struct AttemptTrace {
    frame: RecordFrame,
    gate: RecordingGate,
    confirmed: bool,
}

struct SpyControl {
    fault: FaultPlan,
    // A drain emits at most three records. This buffer is cleared into the
    // caller-owned test trace after every call; it never stores archive history.
    attempts: Vec<AttemptTrace>,
}

trait RecordSink {
    fn fault_plan(&self) -> FaultPlan {
        FaultPlan::None
    }
    fn observe(&mut self, attempts: Vec<AttemptTrace>);
}

#[derive(Default)]
struct MemorySink {
    frames: Vec<RecordFrame>,
    achieved: Option<RecordingGate>,
}

impl MemorySink {
    fn with_achieved(achieved: RecordingGate) -> Self {
        Self {
            frames: Vec::new(),
            achieved: Some(achieved),
        }
    }
}

impl RecordSink for MemorySink {
    fn fault_plan(&self) -> FaultPlan {
        self.achieved.map_or(FaultPlan::None, FaultPlan::Weak)
    }
    fn observe(&mut self, attempts: Vec<AttemptTrace>) {
        self.frames
            .extend(attempts.into_iter().map(|attempt| attempt.frame));
    }
}

struct TestWalWriter {
    writer: WalWriter,
    control: Rc<RefCell<SpyControl>>,
}

impl session::SessionRecordWriter for TestWalWriter {
    fn persist(
        &mut self,
        frame: &RecordFrame,
        gate: RecordingGate,
    ) -> Result<session::PersistenceReceipt, session::PersistError> {
        let injected = {
            let mut control = self.control.borrow_mut();
            let fault = match &mut control.fault {
                FaultPlan::None => None,
                FaultPlan::Weak(achieved) if !achieved.covers(gate) => {
                    Some(EpochPersistenceFault::InsufficientGate)
                }
                FaultPlan::Weak(_) => None,
                FaultPlan::Epoch {
                    fault_at,
                    seen,
                    fault,
                } if matches!(
                    &frame.value,
                    Record::Control(ControlRecord {
                        value: Control::EpochAdvance { .. },
                        ..
                    })
                ) =>
                {
                    *seen += 1;
                    (*seen == *fault_at).then_some(*fault)
                }
                FaultPlan::Epoch { .. } => None,
                FaultPlan::Timer(fault) => {
                    assert!(matches!(
                        &frame.value,
                        Record::Control(ControlRecord {
                            value: Control::Timer { .. },
                            ..
                        })
                    ));
                    Some(*fault)
                }
            };
            assert!(control.attempts.len() < 3, "bounded drain trace");
            control.attempts.push(AttemptTrace {
                frame: frame.clone(),
                gate,
                confirmed: false,
            });
            fault
        };
        if let Some(fault) = injected {
            return match fault {
                EpochPersistenceFault::Persist => {
                    Err(session::PersistError::new("injected persistence error"))
                }
                EpochPersistenceFault::ReceiptMismatch => Ok(session::PersistenceReceipt {
                    through: frame.record_no.checked_next().expect("test mismatch"),
                    achieved_gate: gate,
                }),
                EpochPersistenceFault::InsufficientGate => Ok(session::PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: RecordingGate::Written,
                }),
            };
        }
        self.writer
            .append(frame)
            .map_err(|error| session::PersistError::new(error.to_string()))?;
        // A stronger real receipt is truthful and keeps read-only offline trace
        // inspection independent of a low-level appendable writer export.
        let (through, achieved_gate) = if gate == RecordingGate::Durable {
            (
                self.writer
                    .sync_all()
                    .map_err(|error| session::PersistError::new(error.to_string()))?
                    .durable,
                RecordingGate::Durable,
            )
        } else {
            (
                self.writer
                    .flush()
                    .map_err(|error| session::PersistError::new(error.to_string()))?
                    .flushed,
                RecordingGate::Flushed,
            )
        };
        self.control
            .borrow_mut()
            .attempts
            .last_mut()
            .expect("attempt")
            .confirmed = true;
        Ok(session::PersistenceReceipt {
            through: through.expect("real writer frontier"),
            achieved_gate,
        })
    }
}

struct PublicWsSupervisor {
    inner: BoundSupervisor,
    authority: session::CaptureSessionAuthority,
    turn: session::SessionTurn,
    bound_sink: session::BoundRecordSink,
    control: Rc<RefCell<SpyControl>>,
    owner: Option<CaptureSessionOwner>,
    trace_next_record: u64,
    temp: TempWal,
}

impl PublicWsSupervisor {
    fn new(config: WsSupervisorConfig) -> Result<Self, SupervisorError> {
        Self::build(config, false)
    }
    fn canonical_new(config: WsSupervisorConfig) -> Result<Self, SupervisorError> {
        Self::build(config, true)
    }
    fn build(
        mut config: WsSupervisorConfig,
        canonical_owner: bool,
    ) -> Result<Self, SupervisorError> {
        let temp = TempWal::new("bound-supervisor");
        let mut bootstrap_bindings = config.streams.clone();
        // Invalid canonical duplicate bindings must reach the supervisor's
        // rejection boundary; their fixture still starts from a valid WAL.
        for index in 0..bootstrap_bindings.len() {
            if bootstrap_bindings[..index]
                .iter()
                .any(|prior| prior.spec.instrument == bootstrap_bindings[index].spec.instrument)
            {
                bootstrap_bindings[index].spec.instrument.native_symbol =
                    id(Token::new(&format!("FIXTURE{}", index)));
            }
        }
        let prefix = bootstrap_prefix_many(&bootstrap_bindings, config.recording_gate);
        let actual_next = prefix
            .last()
            .expect("bootstrap")
            .record_no
            .checked_next()
            .expect("next");
        if config.next_record_no.get() == 5 {
            config.next_record_no = actual_next;
        }
        let scopes: Vec<_> = config
            .streams
            .iter()
            .map(|binding| session::ScopeBinding {
                stream: binding.id,
                connection: binding.connection_id,
                epoch: binding.tag.connection,
            })
            .collect();
        let budget = session::RetentionBudget {
            item_cap: config.queue_policy.max_total_items,
            raw_frame_limit: config.queue_policy.max_raw_frames_per_stream,
            raw_byte_limit: config.queue_policy.max_raw_bytes_per_stream,
            max_message_bytes: config.queue_policy.max_raw_message_bytes,
        };
        let control = Rc::new(RefCell::new(SpyControl {
            fault: FaultPlan::None,
            attempts: Vec::with_capacity(3),
        }));
        let (owner, authority, turn, handle, bound_sink) = if canonical_owner {
            let (mut owner, mut turn) = CaptureSessionOwner::create_new(
                &temp.path,
                &prefix[0],
                BoundedCaptureProfile::new(&prefix[1..]),
            )
            .expect("accepted bounded bootstrap");
            let (handle, sink) = owner
                .register_supervisor(&mut turn, &scopes, budget)
                .expect("register owner");
            let authority = sink.authority().clone();
            (Some(owner), authority, turn, handle, sink)
        } else {
            let Record::ArchiveStart(start) = &prefix[0].value else {
                unreachable!()
            };
            let (authority, mut turn) =
                session::CaptureSessionAuthority::new(session::SessionBinding {
                    archive: start.archive,
                    session: start.session,
                    clock: start.clock,
                });
            let handle = authority
                .register_supervisor(
                    &mut turn,
                    &scopes,
                    budget,
                    session::PrefixBinding {
                        context: config.active_context,
                        recording_gate: config.recording_gate,
                        segment: config.segment_no,
                        next_record: actual_next,
                    },
                )
                .map_err(SupervisorError::Authority)?;
            authority
                .set_accepted_stream_bindings(&mut turn, &bootstrap_bindings)
                .expect("frozen accepted stream registry");
            let mut writer = WalWriter::create(&temp.path).expect("fresh real WAL");
            for frame in &prefix {
                writer.append(frame).expect("accepted bootstrap");
            }
            writer.flush().expect("visible bootstrap");
            let sink = authority
                .bind_sink(
                    &mut turn,
                    Box::new(TestWalWriter {
                        writer,
                        control: Rc::clone(&control),
                    }),
                )
                .map_err(SupervisorError::Authority)?;
            (None, authority, turn, handle, sink)
        };
        let inner = BoundSupervisor::new(config, handle)?;
        Ok(Self {
            inner,
            authority,
            turn,
            bound_sink,
            control,
            owner,
            trace_next_record: actual_next.get(),
            temp,
        })
    }
    fn assert_bounds(&self) {
        let report = self.inner.retention_report();
        let ownership = report.ownership;
        assert_eq!(ownership.reserved_archive, 1);
        assert_eq!(
            ownership.work_limit + ownership.reserved_scopes + 1,
            ownership.item_cap
        );
        assert!(
            ownership.work_used + ownership.reserved_scopes + ownership.reserved_archive
                <= ownership.item_cap
        );
        assert_eq!(
            ownership.before_failure + ownership.pre_cut + ownership.post_cut,
            ownership.work_used
        );
        assert!(ownership.work_references <= ownership.work_used * 4);
        assert!(ownership.metadata_backing_bytes <= ownership.metadata_ceiling_bytes);
        assert!(
            report.supervisor_metadata_backing_bytes <= report.supervisor_metadata_ceiling_bytes
        );
        assert!(
            report
                .raw
                .iter()
                .map(|raw| raw.allocated_bytes)
                .sum::<usize>()
                <= report.payload_ceiling_bytes
        );
    }
    fn assert_disposition(&self, disposition: session::SessionDisposition) {
        assert_eq!(
            matches!(
                disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ),
            self.authority.status().failed,
            "caller sees irreversible diagnostic status outside Result"
        );
    }
    fn dispatch_views(
        &mut self,
        commands: impl IntoIterator<Item = session::CommandLease>,
    ) -> Vec<TransportCommand> {
        commands
            .into_iter()
            .map(|command| {
                let connection = command.connection();
                let epoch = command.epoch();
                let view = match command.kind() {
                    session::CommandKind::Connect { endpoint } => TransportCommand::Connect {
                        connection,
                        epoch,
                        endpoint,
                    },
                    session::CommandKind::SendText { text } => TransportCommand::SendText {
                        connection,
                        epoch,
                        text: text.clone(),
                    },
                    session::CommandKind::Close => TransportCommand::Close { connection, epoch },
                    session::CommandKind::ReconnectAfter { delay_ns } => {
                        TransportCommand::ReconnectAfter {
                            connection,
                            epoch,
                            delay_ns: *delay_ns,
                        }
                    }
                };
                assert!(
                    matches!(
                        self.authority
                            .dispatch(&mut self.turn, command, |_| Ok::<(), ()>(())),
                        session::DispatchReport::Dispatched
                    ),
                    "canonical dispatch"
                );
                view
            })
            .collect()
    }
    fn start_commands(&mut self) -> Result<Vec<TransportCommand>, SupervisorError> {
        let report = self.inner.start_commands(&mut self.turn);
        self.assert_disposition(report.session_disposition);
        let commands = self.dispatch_views(report.commands);
        self.assert_bounds();
        report.outcome.map(|_| commands)
    }
    fn queue_text(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        bytes: Vec<u8>,
    ) -> Result<(), SupervisorError> {
        let report = self
            .inner
            .queue_text(&mut self.turn, connection, epoch, stamp, &bytes);
        self.assert_disposition(report.session_disposition);
        self.dispatch_views(report.commands);
        self.assert_bounds();
        report.outcome.map(|_| ())
    }
    fn queue_connected(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> Result<(), SupervisorError> {
        let report = self
            .inner
            .queue_connected(&mut self.turn, connection, epoch, stamp);
        self.assert_disposition(report.session_disposition);
        self.dispatch_views(report.commands);
        self.assert_bounds();
        report.outcome.map(|_| ())
    }
    fn queue_disconnected(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> Result<(), SupervisorError> {
        let report = self
            .inner
            .queue_disconnected(&mut self.turn, connection, epoch, stamp);
        self.assert_disposition(report.session_disposition);
        self.dispatch_views(report.commands);
        self.assert_bounds();
        report.outcome.map(|_| ())
    }
    fn queue_tick(&mut self, stamp: ReceiveStamp) -> Result<(), SupervisorError> {
        let report = self.inner.queue_tick(&mut self.turn, stamp);
        self.assert_disposition(report.session_disposition);
        self.dispatch_views(report.commands);
        self.assert_bounds();
        report.outcome.map(|_| ())
    }
    fn drain_one(
        &mut self,
        sink: &mut impl RecordSink,
    ) -> Result<Option<DrainResult>, SupervisorError> {
        self.control.borrow_mut().fault = sink.fault_plan();
        let report = self.inner.drain_one(&mut self.turn, &mut self.bound_sink);
        self.assert_disposition(report.session_disposition);
        let outcome = report.outcome;
        let attempts = if self.owner.is_some() {
            let mut reader = WalReader::open(&self.temp.path).expect("canonical prefix reader");
            let mut attempts = Vec::new();
            while let Some(frame) = reader.next_record().expect("accepted canonical prefix") {
                if frame.record_no.get() >= self.trace_next_record {
                    self.trace_next_record = frame.record_no.get() + 1;
                    attempts.push(AttemptTrace {
                        frame,
                        gate: RecordingGate::Durable,
                        confirmed: true,
                    });
                }
            }
            attempts
        } else {
            let mut control = self.control.borrow_mut();
            std::mem::replace(&mut control.attempts, Vec::with_capacity(3))
        };
        sink.observe(attempts);
        self.assert_bounds();
        outcome.map(|result| {
            result.map(|mut result| DrainResult {
                records: std::mem::take(&mut result.records).into_vec(),
                commands: self.dispatch_views(std::mem::take(&mut result.commands)),
                events: std::mem::take(&mut result.events).into_vec(),
            })
        })
    }
    fn snapshot(&self, stream: StreamId) -> Option<StreamSupervisorSnapshot> {
        self.inner.snapshot(stream)
    }
    fn queued_items(&self) -> usize {
        self.inner.queued_items()
    }
    fn is_halted(&self) -> bool {
        self.inner.is_halted()
    }
}

struct TempWal {
    path: PathBuf,
}

impl TempWal {
    fn new(label: &str) -> Self {
        let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "proscalping-rec001d-{}-{label}-{serial}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn id<T>(value: Result<T, IdentityError>) -> T {
    value.expect("valid test identity")
}

fn stream_binding(stream: u32, connection: u32, book: u32, symbol: &str) -> StreamBinding {
    let spec = SpecVersion::new(1).expect("positive spec");
    StreamBinding {
        id: id(StreamId::new(stream)),
        instrument_slot: id(InstrumentSlot::new(stream)),
        spec: SpecRef {
            instrument: InstrumentRef {
                venue: id(Token::new("bitget")),
                market: MarketKind::Perpetual,
                product_namespace: id(Token::new("usdt-futures")),
                native_symbol: id(Token::new(symbol)),
            },
            version: spec,
        },
        connection_id: id(ConnectionId::new(connection)),
        channel: Channel::BookNormal,
        book_id: Some(id(BookId::new(book))),
        tag: EpochTag {
            spec,
            connection: id(ConnectionEpoch::new(1)),
            subscription: id(SubscriptionEpoch::new(1)),
            book: Some(id(BookEpoch::new(1))),
        },
        feed_profile: id(FeedProfileVersion::new(1)),
    }
}

fn active_context() -> ActiveContext {
    ActiveContext {
        config: id(ConfigVersion::new(1)),
        normalizer: id(NormalizerVersion::new(1)),
    }
}

fn supervisor_config_with_record(
    streams: Vec<StreamBinding>,
    queue_policy: QueuePolicy,
    gate: RecordingGate,
    next_record_no: RecordNo,
) -> WsSupervisorConfig {
    WsSupervisorConfig {
        active_context: active_context(),
        recording_gate: gate,
        segment_no: SegmentNo::new(0),
        next_record_no,
        queue_policy,
        streams,
    }
}

fn supervisor_config(
    streams: Vec<StreamBinding>,
    queue_policy: QueuePolicy,
    gate: RecordingGate,
) -> WsSupervisorConfig {
    supervisor_config_with_record(streams, queue_policy, gate, id(RecordNo::new(5)))
}

fn supervisor(
    streams: Vec<StreamBinding>,
    queue_policy: QueuePolicy,
    gate: RecordingGate,
) -> PublicWsSupervisor {
    PublicWsSupervisor::new(supervisor_config(streams, queue_policy, gate))
        .expect("valid supervisor config")
}

fn canonical_supervisor(
    streams: Vec<StreamBinding>,
    queue_policy: QueuePolicy,
    gate: RecordingGate,
) -> PublicWsSupervisor {
    PublicWsSupervisor::canonical_new(supervisor_config(streams, queue_policy, gate))
        .expect("valid canonical owner/supervisor")
}

fn stamp(monotonic_ns: u64) -> ReceiveStamp {
    ReceiveStamp {
        unix_ns: 1_800_000_000_000_000_000_i64 + monotonic_ns as i64,
        monotonic_ns,
    }
}

fn ack(symbol: &str) -> Vec<u8> {
    format!(
        r#"{{"event":"subscribe","arg":{{"instType":"usdt-futures","topic":"books50","symbol":"{symbol}"}},"connId":"offline"}}"#
    )
    .into_bytes()
}

fn subscription_error() -> Vec<u8> {
    br#"{"event":"error","code":"30001","msg":"synthetic failure"}"#.to_vec()
}

fn snapshot(symbol: &str, quantity: &str) -> Vec<u8> {
    format!(
        r#"{{"action":"snapshot","arg":{{"instType":"usdt-futures","topic":"books50","symbol":"{symbol}"}},"data":[{{"a":[["101","{quantity}"]],"b":[["100","2"]],"pseq":0,"seq":10,"ts":"1000"}}],"ts":1000}}"#
    )
    .into_bytes()
}

fn update(symbol: &str, pseq: u64, seq: u64) -> Vec<u8> {
    format!(
        r#"{{"action":"update","arg":{{"instType":"usdt-futures","topic":"books50","symbol":"{symbol}"}},"data":[{{"a":[["102","3"]],"b":[],"pseq":{pseq},"seq":{seq},"ts":"1010"}}],"ts":1010}}"#
    )
    .into_bytes()
}

fn connect_one(
    supervisor: &mut PublicWsSupervisor,
    sink: &mut impl RecordSink,
    binding: &StreamBinding,
    at: u64,
) -> DrainResult {
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(at))
        .expect("queue connected");
    supervisor
        .drain_one(sink)
        .expect("drain connected")
        .expect("connected result")
}

fn drain_all(supervisor: &mut PublicWsSupervisor, sink: &mut impl RecordSink) -> Vec<DrainResult> {
    let mut results = Vec::new();
    while let Some(result) = supervisor.drain_one(sink).expect("drain") {
        results.push(result);
    }
    results
}

#[test]
fn supervisor_rejects_bindings_outside_exact_bitget_usdt_futures_profile() {
    let base = stream_binding(1, 1, 1, "BTCUSDT");

    let mut wrong_venue = base.clone();
    wrong_venue.spec.instrument.venue = id(Token::new("other"));
    assert!(matches!(
        PublicWsSupervisor::new(supervisor_config(
            vec![wrong_venue],
            QueuePolicy::default(),
            RecordingGate::Written,
        )),
        Err(SupervisorError::InvalidConfiguration(
            "regular bitget usdt-futures books50 binding"
        ))
    ));

    for namespace in ["coin-futures", "usdc-futures", "other-futures"] {
        let mut wrong_namespace = base.clone();
        wrong_namespace.spec.instrument.product_namespace = id(Token::new(namespace));
        assert!(matches!(
            PublicWsSupervisor::new(supervisor_config(
                vec![wrong_namespace],
                QueuePolicy::default(),
                RecordingGate::Written,
            )),
            Err(SupervisorError::InvalidConfiguration(
                "regular bitget usdt-futures books50 binding"
            ))
        ));
    }

    let mut wrong_market = base.clone();
    wrong_market.spec.instrument.market = MarketKind::Spot;
    assert!(matches!(
        PublicWsSupervisor::new(supervisor_config(
            vec![wrong_market],
            QueuePolicy::default(),
            RecordingGate::Written,
        )),
        Err(SupervisorError::InvalidConfiguration(
            "regular bitget usdt-futures books50 binding"
        ))
    ));

    let mut dated = base;
    dated.spec.instrument.market = MarketKind::DatedFuture;
    assert!(
        PublicWsSupervisor::new(supervisor_config(
            vec![dated],
            QueuePolicy::default(),
            RecordingGate::Written,
        ))
        .is_ok()
    );
}

#[test]
fn supervisor_rejects_second_writer_for_same_canonical_normal_book() {
    let first = stream_binding(1, 1, 1, "BTCUSDT");
    let second = stream_binding(2, 2, 2, "BTCUSDT");

    // The canonical API rejects mutation of the accepted fresh registry before
    // capture starts. Core identity arithmetic remains independently tested.
    assert!(matches!(
        PublicWsSupervisor::new(supervisor_config(
            vec![first, second],
            QueuePolicy::default(),
            RecordingGate::Written,
        )),
        Err(SupervisorError::Authority(
            session::AuthorityError::InvalidBinding
        ))
    ));
}

#[test]
fn previous_epoch_raw_exceeding_wal_capacity_is_bounded_as_empty_diagnostic() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_epoch = binding.tag.connection;
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 1_000_000,
        max_raw_message_bytes: 1_000_000,
        max_total_items: 5,
    };
    let mut supervisor = supervisor(vec![binding.clone()], queue, RecordingGate::Written);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(2))
        .expect("disconnect");
    supervisor
        .drain_one(&mut sink)
        .expect("drain disconnect")
        .expect("disconnect result");
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("pending")
            .tag
            .connection,
        old_epoch
    );
    supervisor
        .drain_one(&mut sink)
        .expect("complete disconnect")
        .expect("completion result");
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("current")
            .tag
            .connection
            .get(),
        2
    );

    supervisor
        .queue_text(
            binding.connection_id,
            old_epoch,
            stamp(3),
            vec![b'x'; 1_100_000],
        )
        .expect("bounded stale rejection");
    let queued = supervisor.snapshot(binding.id).expect("queued");
    assert_eq!(queued.queued_raw_frames, 0);
    assert_eq!(queued.queued_raw_bytes, 0);
    assert_eq!(supervisor.queued_items(), 1);

    let result = supervisor
        .drain_one(&mut sink)
        .expect("drain rejected stale")
        .expect("rejected stale result");
    assert_eq!(result.records.len(), 2);

    let raw = sink
        .frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.value {
            Record::RawInput(raw) if raw.tag.connection == old_epoch => Some(raw),
            _ => None,
        })
        .expect("bounded stale raw provenance");
    assert!(raw.bytes.is_empty());
    assert_eq!(raw.attempt.get(), 1);

    let gap = sink
        .frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.value {
            Record::Gap(gap) if gap.reason == Reason::Unknown => Some(gap),
            _ => None,
        })
        .expect("stale rejection diagnostic");
    let GapScope::ExplicitTargets(targets) = &gap.scope else {
        panic!("expected explicit stale diagnostic target");
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].stream, binding.id);
    assert_eq!(targets[0].tag.connection, old_epoch);
    assert_eq!(targets[0].range, None);
    assert_eq!(targets[0].loss_count, None);
    assert!(!supervisor.is_halted());
}

#[test]
fn sustained_overflow_coalesces_loss_and_does_not_halt_neighbor_stream() {
    let a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 12,
    };
    let mut supervisor = supervisor(vec![a.clone(), b.clone()], queue, RecordingGate::Written);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &a, 1);
    connect_one(&mut supervisor, &mut sink, &b, 2);

    supervisor
        .queue_text(a.connection_id, a.tag.connection, stamp(10), ack("BTCUSDT"))
        .expect("a first raw");
    for n in 0..32 {
        supervisor
            .queue_text(
                a.connection_id,
                a.tag.connection,
                stamp(11 + n),
                ack("BTCUSDT"),
            )
            .expect("a coalesced overflow");
    }
    assert_eq!(supervisor.queued_items(), 2);
    assert!(!supervisor.is_halted());

    supervisor
        .queue_text(
            b.connection_id,
            b.tag.connection,
            stamp(100),
            ack("ETHUSDT"),
        )
        .expect("neighbor raw remains admissible");
    assert_eq!(supervisor.queued_items(), 3);

    drain_all(&mut supervisor, &mut sink);
    assert!(!supervisor.is_halted());

    let overflow = sink
        .frames
        .iter()
        .find_map(|frame| match &frame.value {
            Record::Gap(gap) if gap.reason == Reason::QueueOverflow => Some(gap),
            _ => None,
        })
        .expect("coalesced overflow gap");
    let GapScope::ExplicitTargets(targets) = &overflow.scope else {
        panic!("expected explicit overflow target");
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].stream, a.id);
    assert_eq!(
        targets[0]
            .range
            .map(|(first, last)| (first.get(), last.get())),
        Some((2, 33))
    );
    assert_eq!(targets[0].loss_count, Some(32));

    let b_state = supervisor.snapshot(b.id).expect("neighbor");
    assert_eq!(b_state.transport, Transport::Up);
    assert_eq!(b_state.subscription, SubscriptionState::AwaitingSnapshot);
}

#[test]
fn queued_overflow_before_disconnect_drains_before_epoch_advance() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_tag = binding.tag;
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 5,
    };
    let mut supervisor = supervisor(vec![binding.clone()], queue, RecordingGate::Written);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(10),
            ack("BTCUSDT"),
        )
        .expect("admitted raw");
    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(11))
        .expect("queued disconnect");
    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(12),
            ack("BTCUSDT"),
        )
        .expect("queued overflow behind disconnect");

    supervisor
        .drain_one(&mut sink)
        .expect("drain raw")
        .expect("raw result");
    let down = supervisor
        .drain_one(&mut sink)
        .expect("drain disconnect")
        .expect("disconnect result");
    assert_eq!(down.records.len(), 1);
    let pending = supervisor.snapshot(stream).expect("pending disconnect");
    assert_eq!(pending.tag, old_tag);
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);
    assert!(!supervisor.is_halted());

    let loss = supervisor
        .drain_one(&mut sink)
        .expect("drain loss")
        .expect("loss result");
    assert_eq!(loss.records.len(), 1);
    assert!(
        loss.events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::QueueGapRecorded { .. }))
    );
    assert!(loss.commands.is_empty());
    assert_eq!(
        supervisor.snapshot(stream).expect("pending after loss").tag,
        old_tag
    );
    assert_eq!(supervisor.queued_items(), 0);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete after loss")
        .expect("completion result");
    assert_eq!(completed.records.len(), 3);
    assert!(completed.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::EpochAdvanced { tag, .. } if tag.connection.get() == 2
    )));

    let gap = sink
        .frames
        .iter()
        .find_map(|frame| match &frame.value {
            Record::Gap(gap) if gap.reason == Reason::QueueOverflow => Some(gap),
            _ => None,
        })
        .expect("queued overflow persisted");
    let GapScope::ExplicitTargets(targets) = &gap.scope else {
        panic!("expected explicit overflow target");
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].tag, old_tag);
    assert_eq!(
        targets[0]
            .range
            .map(|(first, last)| (first.get(), last.get())),
        Some((2, 2))
    );

    let current = supervisor.snapshot(stream).expect("advanced");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.tag.subscription.get(), 2);
    assert_eq!(current.tag.book.expect("book").get(), 2);
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert!(!supervisor.is_halted());
}

#[test]
fn connect_subscribe_and_ack_are_persisted_before_accepted_state() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();

    let commands = supervisor.start_commands().expect("start");
    assert_eq!(
        commands,
        vec![TransportCommand::Connect {
            connection: binding.connection_id,
            epoch: binding.tag.connection,
            endpoint: BITGET_PUBLIC_WS_ENDPOINT,
        }]
    );

    let connected = connect_one(&mut supervisor, &mut sink, &binding, 100);
    assert_eq!(connected.records.len(), 1);
    assert_eq!(
        connected.commands,
        vec![TransportCommand::SendText {
            connection: binding.connection_id,
            epoch: binding.tag.connection,
            text: r#"{"op":"subscribe","args":[{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"}]}"#.to_owned(),
        }]
    );
    let after_up = supervisor.snapshot(stream).expect("snapshot");
    assert_eq!(after_up.transport, Transport::Up);
    assert_eq!(after_up.subscription, SubscriptionState::AwaitingAck);

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(101),
            ack("BTCUSDT"),
        )
        .expect("queue ack");
    let accepted = supervisor
        .drain_one(&mut sink)
        .expect("drain ack")
        .expect("ack result");
    assert_eq!(accepted.records.len(), 1);
    assert!(matches!(
        accepted.events.as_slice(),
        [SupervisorEvent::SubscriptionAccepted { stream: event_stream, .. }]
            if *event_stream == stream
    ));
    let snapshot = supervisor.snapshot(stream).expect("snapshot");
    assert_eq!(snapshot.subscription, SubscriptionState::AwaitingSnapshot);
    assert!(matches!(
        sink.frames[0].value,
        Record::Control(ControlRecord {
            value: Control::Transport {
                value: Transport::Up,
                ..
            },
            ..
        })
    ));
    assert!(matches!(sink.frames[1].value, Record::RawInput(_)));
}

#[test]
fn heartbeat_pong_records_transport_only_and_does_not_create_market_freshness() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1_000);

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(1_001),
            ack("BTCUSDT"),
        )
        .expect("ack");
    supervisor.drain_one(&mut sink).expect("ack drain");

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(1_002),
            snapshot("BTCUSDT", "1"),
        )
        .expect("snapshot");
    supervisor.drain_one(&mut sink).expect("snapshot drain");
    let before = supervisor.snapshot(stream).expect("before pong");
    assert_eq!(before.subscription, SubscriptionState::CapturingRaw);
    let market_record = before.last_market_record.expect("market record");

    let ping_due = 1_000 + HEARTBEAT_INTERVAL_NS;
    supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
    let ping = supervisor
        .drain_one(&mut sink)
        .expect("drain ping")
        .expect("ping result");
    assert!(ping.commands.iter().any(|command| {
        matches!(
            command,
            TransportCommand::SendText { text, .. } if text == "ping"
        )
    }));

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(ping_due + 1),
            b"pong".to_vec(),
        )
        .expect("queue pong");
    supervisor.drain_one(&mut sink).expect("drain pong");
    let after = supervisor.snapshot(stream).expect("after pong");
    assert_eq!(after.transport, Transport::Up);
    assert_eq!(after.subscription, SubscriptionState::CapturingRaw);
    assert_eq!(after.last_market_record, Some(market_record));
}

#[test]
fn disconnect_reconnect_advances_connection_subscription_and_book_epochs() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(200))
        .expect("queue disconnect");
    let disconnected = supervisor
        .drain_one(&mut sink)
        .expect("drain disconnect")
        .expect("disconnect result");
    assert_eq!(disconnected.records.len(), 1);
    assert!(matches!(
        disconnected.commands.as_slice(),
        [TransportCommand::Close { epoch, .. }] if *epoch == binding.tag.connection
    ));
    let pending = supervisor.snapshot(stream).expect("pending disconnect");
    assert_eq!(pending.tag, binding.tag);
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete disconnect")
        .expect("completion result");
    assert_eq!(completed.records.len(), 3);
    let current = supervisor.snapshot(stream).expect("snapshot");
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.tag.subscription.get(), 2);
    assert_eq!(current.tag.book.expect("book epoch").get(), 2);

    let (delay, next_epoch) = completed
        .commands
        .iter()
        .find_map(|command| match command {
            TransportCommand::ReconnectAfter {
                epoch, delay_ns, ..
            } => Some((*delay_ns, *epoch)),
            _ => None,
        })
        .expect("reconnect command");
    assert!((RECONNECT_BASE_NS_V1..=RECONNECT_MAX_NS_V1).contains(&delay));

    supervisor
        .queue_connected(binding.connection_id, next_epoch, stamp(200 + delay))
        .expect("queue reconnected");
    supervisor.drain_one(&mut sink).expect("drain reconnected");
    let reconnected = supervisor.snapshot(stream).expect("snapshot");
    assert_eq!(reconnected.transport, Transport::Up);
    assert_eq!(reconnected.subscription, SubscriptionState::AwaitingAck);
}

#[test]
fn repeated_same_epoch_disconnect_records_duplicate_down_and_finishes_once() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(10))
        .expect("first disconnect");
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(11))
        .expect("duplicate disconnect");

    let first = supervisor
        .drain_one(&mut sink)
        .expect("drain first disconnect")
        .expect("first disconnect result");
    assert_eq!(first.records.len(), 1);
    assert_eq!(
        first
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::Close { .. }))
            .count(),
        1
    );
    assert!(
        !first
            .commands
            .iter()
            .any(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
    );
    let pending = supervisor.snapshot(stream).expect("pending");
    assert_eq!(pending.tag.connection, old_epoch);
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);

    let second = supervisor
        .drain_one(&mut sink)
        .expect("drain duplicate disconnect")
        .expect("duplicate disconnect result");
    assert_eq!(second.records.len(), 1);
    assert!(second.commands.is_empty());
    assert!(
        second
            .events
            .iter()
            .all(|event| !matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor
            .snapshot(stream)
            .expect("pending duplicate")
            .tag
            .connection,
        old_epoch
    );
    assert_eq!(supervisor.queued_items(), 0);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete duplicate disconnect")
        .expect("completion result");
    assert_eq!(
        first
            .commands
            .iter()
            .chain(second.commands.iter())
            .chain(completed.commands.iter())
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        completed
            .events
            .iter()
            .filter(|event| matches!(event, SupervisorEvent::EpochAdvanced { .. }))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Down,
                        ..
                    },
                    ..
                })
            ))
            .count(),
        2
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ))
            .count(),
        3
    );

    let current = supervisor.snapshot(stream).expect("advanced");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.tag.subscription.get(), 2);
    assert_eq!(current.tag.book.expect("book").get(), 2);
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(current.last_market_record, None);
    assert_eq!(supervisor.queued_items(), 0);
    assert!(
        supervisor
            .drain_one(&mut sink)
            .expect("empty drain after completion")
            .is_none()
    );
    assert!(!supervisor.is_halted());
}

#[test]
fn heartbeat_timeout_then_same_epoch_disconnect_finishes_one_transition() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;
    supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
    supervisor
        .drain_one(&mut sink)
        .expect("drain ping")
        .expect("ping result");

    let timeout = ping_due + PONG_TIMEOUT_NS_V1;
    supervisor
        .queue_tick(stamp(timeout))
        .expect("queue pong timeout");
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(timeout + 1))
        .expect("queue same-epoch disconnect");

    let timed_out = supervisor
        .drain_one(&mut sink)
        .expect("drain timeout")
        .expect("timeout result");
    assert_eq!(timed_out.records.len(), 2);
    assert!(
        timed_out
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::HeartbeatTimerRecorded { .. }))
    );
    assert!(
        !timed_out
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    let pending = supervisor.snapshot(stream).expect("pending timeout");
    assert_eq!(pending.tag.connection, old_epoch);
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);

    let duplicate = supervisor
        .drain_one(&mut sink)
        .expect("drain queued disconnect")
        .expect("queued disconnect result");
    assert_eq!(duplicate.records.len(), 1);
    assert!(duplicate.commands.is_empty());
    assert!(
        duplicate
            .events
            .iter()
            .all(|event| !matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor
            .snapshot(stream)
            .expect("pending duplicate")
            .tag
            .connection,
        old_epoch
    );
    assert_eq!(supervisor.queued_items(), 0);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete timeout disconnect")
        .expect("completion result");
    assert_eq!(
        timed_out
            .commands
            .iter()
            .chain(duplicate.commands.iter())
            .chain(completed.commands.iter())
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Timer { .. },
                    ..
                })
            ))
            .count(),
        2
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Down,
                        ..
                    },
                    ..
                })
            ))
            .count(),
        2
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ))
            .count(),
        3
    );

    let current = supervisor.snapshot(stream).expect("advanced");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.tag.subscription.get(), 2);
    assert_eq!(current.tag.book.expect("book").get(), 2);
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(supervisor.queued_items(), 0);
    assert!(!supervisor.is_halted());
}

#[test]
fn repeated_disconnect_waits_for_queued_old_generation_raw_before_advancing() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_tag = binding.tag;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    let raw = ack("BTCUSDT");
    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(10))
        .expect("first disconnect");
    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(11),
            raw.clone(),
        )
        .expect("old-generation raw");
    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(12))
        .expect("second disconnect");

    let first = supervisor
        .drain_one(&mut sink)
        .expect("drain first disconnect")
        .expect("first result");
    assert_eq!(first.records.len(), 1);
    assert_eq!(supervisor.snapshot(stream).expect("pending").tag, old_tag);

    let raw_result = supervisor
        .drain_one(&mut sink)
        .expect("drain old raw")
        .expect("old raw result");
    assert!(
        raw_result
            .events
            .iter()
            .all(|event| !matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor.snapshot(stream).expect("still pending").tag,
        old_tag
    );

    let duplicate = supervisor
        .drain_one(&mut sink)
        .expect("drain duplicate")
        .expect("duplicate result");
    assert_eq!(duplicate.records.len(), 1);
    assert!(duplicate.commands.is_empty());
    assert_eq!(
        supervisor.snapshot(stream).expect("pending duplicate").tag,
        old_tag
    );
    assert_eq!(supervisor.queued_items(), 0);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete after duplicate")
        .expect("completion result");
    assert!(completed.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::EpochAdvanced { tag, .. } if tag.connection.get() == 2
    )));

    let raw_position = sink
        .frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.value,
                Record::RawInput(input) if input.tag == old_tag && input.bytes == raw
            )
        })
        .expect("old raw persisted");
    let advance_position = sink
        .frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance {
                        change: EpochChange::Connection { .. },
                        ..
                    },
                    ..
                })
            )
        })
        .expect("connection epoch advance");
    assert!(raw_position < advance_position);

    let current = supervisor.snapshot(stream).expect("advanced");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(supervisor.queued_items(), 0);
    assert!(!supervisor.is_halted());
}

#[test]
fn pending_disconnect_finishes_when_last_blocker_drain_empties_queue() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_tag = binding.tag;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(10))
        .expect("disconnect");
    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(11),
            ack("BTCUSDT"),
        )
        .expect("last old-generation blocker");

    let down = supervisor
        .drain_one(&mut sink)
        .expect("drain down")
        .expect("down result");
    assert_eq!(down.records.len(), 1);
    assert_eq!(supervisor.snapshot(stream).expect("pending").tag, old_tag);
    assert_eq!(supervisor.queued_items(), 1);

    let last = supervisor
        .drain_one(&mut sink)
        .expect("drain last blocker")
        .expect("last blocker result");
    assert!(last.commands.is_empty());
    assert!(
        last.events
            .iter()
            .all(|event| !matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor
            .snapshot(stream)
            .expect("pending at empty queue")
            .tag,
        old_tag
    );
    assert_eq!(supervisor.queued_items(), 0);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete on empty queue")
        .expect("completion result");
    assert!(completed.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::EpochAdvanced { tag, .. } if tag.connection.get() == 2
    )));
    assert!(completed.commands.iter().any(|command| matches!(
        command,
        TransportCommand::ReconnectAfter { epoch, .. } if epoch.get() == 2
    )));
    assert_eq!(supervisor.queued_items(), 0);

    let current = supervisor.snapshot(stream).expect("advanced");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert!(
        supervisor
            .drain_one(&mut sink)
            .expect("empty drain")
            .is_none()
    );
}

#[test]
fn stale_disconnect_after_epoch_advance_cannot_start_another_transition() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(10))
        .expect("disconnect");
    let first = supervisor
        .drain_one(&mut sink)
        .expect("drain disconnect")
        .expect("disconnect result");
    assert_eq!(first.records.len(), 1);
    assert!(matches!(
        first.commands.as_slice(),
        [TransportCommand::Close { .. }]
    ));
    assert_eq!(
        supervisor.snapshot(stream).expect("pending").tag.connection,
        old_epoch
    );
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete disconnect")
        .expect("completion result");
    assert!(completed.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::EpochAdvanced { tag, .. } if tag.connection.get() == 2
    )));

    let before = supervisor.snapshot(stream).expect("before stale");
    let error = supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(11))
        .expect_err("old epoch disconnect rejected");
    assert_eq!(
        error,
        SupervisorError::UnknownConnectionEpoch {
            connection: binding.connection_id,
            epoch: old_epoch,
        }
    );
    assert_eq!(supervisor.queued_items(), 0);
    assert_eq!(supervisor.snapshot(stream).expect("after stale"), before);
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ))
            .count(),
        3
    );
}

#[test]
fn reconnect_backoff_is_deterministic_and_bounded() {
    let stream = id(StreamId::new(4));
    let mut previous = 0;
    for attempt in 1..=128 {
        let delay = reconnect_delay_ns(attempt, stream);
        assert!((RECONNECT_BASE_NS_V1..=RECONNECT_MAX_NS_V1).contains(&delay));
        assert!(delay >= previous);
        assert_eq!(delay, reconnect_delay_ns(attempt, stream));
        previous = delay;
    }
}

#[test]
fn subscription_failure_records_raw_then_explicit_gap() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 10);

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(11),
            subscription_error(),
        )
        .expect("queue error");
    let result = supervisor
        .drain_one(&mut sink)
        .expect("drain error")
        .expect("error result");
    assert_eq!(result.records.len(), 2);
    assert!(matches!(
        sink.frames[sink.frames.len() - 2].value,
        Record::RawInput(_)
    ));
    assert!(matches!(
        sink.frames.last().expect("gap").value,
        Record::Gap(Gap {
            reason: Reason::Unknown,
            ..
        })
    ));
    assert_eq!(
        supervisor.snapshot(stream).expect("snapshot").subscription,
        SubscriptionState::Degraded
    );
}

#[test]
fn critical_queue_overflow_records_known_attempt_gap_without_evicting_prior_raw() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 5,
    };
    let mut supervisor = supervisor(vec![binding.clone()], queue, RecordingGate::Written);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 10);

    let first = ack("BTCUSDT");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(11),
            first.clone(),
        )
        .expect("first raw");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(12),
            ack("BTCUSDT"),
        )
        .expect("overflow becomes gap");

    let results = drain_all(&mut supervisor, &mut sink);
    assert_eq!(results.len(), 2);
    let raw = sink
        .frames
        .iter()
        .find_map(|frame| match &frame.value {
            Record::RawInput(raw) if raw.bytes == first => Some(raw),
            _ => None,
        })
        .expect("prior raw retained");
    assert_eq!(raw.attempt.get(), 1);

    let gap = sink
        .frames
        .iter()
        .find_map(|frame| match &frame.value {
            Record::Gap(gap) if gap.reason == Reason::QueueOverflow => Some(gap),
            _ => None,
        })
        .expect("queue gap");
    let GapScope::ExplicitTargets(targets) = &gap.scope else {
        panic!("expected explicit target");
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].stream, stream);
    assert_eq!(
        targets[0]
            .range
            .map(|(first, last)| (first.get(), last.get())),
        Some((2, 2))
    );
    assert_eq!(targets[0].loss_count, Some(1));
    assert_eq!(
        supervisor.snapshot(stream).expect("snapshot").subscription,
        SubscriptionState::Degraded
    );
}

#[test]
fn overflow_of_one_stream_does_not_freeze_neighbor_stream() {
    let a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 12,
    };
    let mut supervisor = supervisor(vec![a.clone(), b.clone()], queue, RecordingGate::Written);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &a, 10);
    connect_one(&mut supervisor, &mut sink, &b, 11);

    supervisor
        .queue_text(a.connection_id, a.tag.connection, stamp(20), ack("BTCUSDT"))
        .expect("a first");
    supervisor
        .queue_text(a.connection_id, a.tag.connection, stamp(21), ack("BTCUSDT"))
        .expect("a overflow");
    supervisor
        .queue_text(b.connection_id, b.tag.connection, stamp(22), ack("ETHUSDT"))
        .expect("b first");
    drain_all(&mut supervisor, &mut sink);

    assert_eq!(
        supervisor.snapshot(a.id).expect("a").subscription,
        SubscriptionState::Degraded
    );
    let b_state = supervisor.snapshot(b.id).expect("b");
    assert_eq!(b_state.transport, Transport::Up);
    assert_eq!(b_state.subscription, SubscriptionState::AwaitingSnapshot);
}

#[test]
fn global_receive_order_raw_bytes_and_receive_timestamps_reach_recording_boundary() {
    let a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let mut supervisor = supervisor(
        vec![a.clone(), b.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &a, 1);
    connect_one(&mut supervisor, &mut sink, &b, 2);

    let first = ack("BTCUSDT");
    let second = ack("ETHUSDT");
    supervisor
        .queue_text(a.connection_id, a.tag.connection, stamp(100), first.clone())
        .expect("first");
    supervisor
        .queue_text(
            b.connection_id,
            b.tag.connection,
            stamp(101),
            second.clone(),
        )
        .expect("second");
    drain_all(&mut supervisor, &mut sink);

    let raws: Vec<&RawInput> = sink
        .frames
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::RawInput(raw) => Some(raw),
            _ => None,
        })
        .collect();
    assert_eq!(raws.len(), 2);
    assert_eq!(raws[0].bytes, first);
    assert_eq!(raws[1].bytes, second);
    assert_eq!(raws[0].context.monotonic_ns.get(), 100);
    assert_eq!(raws[1].context.monotonic_ns.get(), 101);
    assert_eq!(raws[0].context.unix_ns.get(), stamp(100).unix_ns);
    assert_eq!(raws[1].context.unix_ns.get(), stamp(101).unix_ns);
}

#[test]
fn zero_quantity_remains_exact_raw_bytes_without_canonical_delete_semantics() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(2),
            ack("BTCUSDT"),
        )
        .expect("ack");
    supervisor.drain_one(&mut sink).expect("ack drain");

    let bytes = snapshot("BTCUSDT", "0");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(3),
            bytes.clone(),
        )
        .expect("snapshot");
    supervisor.drain_one(&mut sink).expect("snapshot drain");

    let raw = sink
        .frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.value {
            Record::RawInput(raw) => Some(raw),
            _ => None,
        })
        .expect("raw frame");
    assert_eq!(raw.bytes, bytes);
}

#[test]
fn old_epoch_raw_is_recorded_but_cannot_revive_current_generation() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(10))
        .expect("disconnect");
    let down = supervisor
        .drain_one(&mut sink)
        .expect("disconnect drain")
        .expect("down result");
    assert!(matches!(
        down.commands.as_slice(),
        [TransportCommand::Close { .. }]
    ));
    assert_eq!(
        supervisor.snapshot(stream).expect("pending").tag.connection,
        old_epoch
    );
    supervisor
        .drain_one(&mut sink)
        .expect("complete disconnect")
        .expect("completion result");
    let current = supervisor.snapshot(stream).expect("current");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.subscription, SubscriptionState::Backoff);

    supervisor
        .queue_text(
            binding.connection_id,
            old_epoch,
            stamp(11),
            snapshot("BTCUSDT", "1"),
        )
        .expect("late old raw");
    let result = supervisor
        .drain_one(&mut sink)
        .expect("old drain")
        .expect("old result");
    assert!(matches!(
        result.events.as_slice(),
        [SupervisorEvent::ObsoleteRawRecorded { tag, .. }] if tag.connection == old_epoch
    ));
    let after = supervisor.snapshot(stream).expect("after");
    assert_eq!(after.tag.connection.get(), 2);
    assert_eq!(after.subscription, SubscriptionState::Backoff);
    assert_eq!(after.last_market_record, None);
}

#[test]
fn continuity_gap_records_source_gap_and_never_heals_from_an_external_snapshot() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(2),
            ack("BTCUSDT"),
        )
        .expect("ack");
    supervisor.drain_one(&mut sink).expect("ack drain");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(3),
            snapshot("BTCUSDT", "1"),
        )
        .expect("snapshot");
    supervisor.drain_one(&mut sink).expect("snapshot drain");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(4),
            update("BTCUSDT", 11, 12),
        )
        .expect("gap update");
    let result = supervisor
        .drain_one(&mut sink)
        .expect("gap drain")
        .expect("gap result");
    assert_eq!(result.records.len(), 2);
    assert!(result.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::SourceGapRecorded {
            reason: Reason::SourceGap,
            ..
        }
    )));
    assert_eq!(
        supervisor.snapshot(stream).expect("snapshot").subscription,
        SubscriptionState::Degraded
    );
}

#[test]
fn heartbeat_timeout_records_timer_down_and_new_generation() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;
    supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
    supervisor.drain_one(&mut sink).expect("drain ping");

    let timeout = ping_due + PONG_TIMEOUT_NS_V1;
    supervisor
        .queue_tick(stamp(timeout))
        .expect("queue timeout");
    let result = supervisor
        .drain_one(&mut sink)
        .expect("drain timeout")
        .expect("timeout result");
    assert_eq!(result.records.len(), 2);
    assert!(matches!(
        result.commands.as_slice(),
        [TransportCommand::Close { .. }]
    ));
    let pending = supervisor.snapshot(stream).expect("pending timeout");
    assert_eq!(pending.tag, binding.tag);
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete timeout")
        .expect("completion result");
    assert_eq!(completed.records.len(), 3);
    assert!(matches!(
        completed.commands.as_slice(),
        [TransportCommand::ReconnectAfter { epoch, .. }] if epoch.get() == 2
    ));
    let state = supervisor.snapshot(stream).expect("state");
    assert_eq!(state.transport, Transport::Unknown);
    assert_eq!(state.tag.connection.get(), 2);
    assert_eq!(state.subscription, SubscriptionState::Backoff);
}

#[test]
fn weak_storage_receipt_halts_before_transport_publication() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Durable,
    );
    let mut sink = MemorySink::with_achieved(RecordingGate::Written);
    supervisor.start_commands().expect("start");
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(1))
        .expect("connected");

    let error = supervisor
        .drain_one(&mut sink)
        .expect_err("weak receipt must fail");
    assert!(matches!(
        error,
        SupervisorError::PersistenceGateTooWeak {
            required: RecordingGate::Durable,
            achieved: RecordingGate::Written
        }
    ));
    assert!(supervisor.is_halted());
    let state = supervisor.snapshot(stream).expect("state");
    assert_eq!(state.transport, Transport::Unknown);
    assert_eq!(state.subscription, SubscriptionState::Connecting);
}

#[test]
fn disconnect_at_max_monotonic_halts_before_down_or_generation_commit() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let stream = binding.id;
    let old_tag = binding.tag;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before_records = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(u64::MAX))
        .expect("queue disconnect");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::TimeOverflow)
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before_records);
    let state = supervisor.snapshot(stream).expect("state");
    assert_eq!(state.tag, old_tag);
    assert_eq!(state.transport, Transport::Up);
    assert_eq!(state.subscription, SubscriptionState::AwaitingAck);
    assert!(supervisor.drain_one(&mut sink).is_err());
}

#[test]
fn disconnect_near_reconnect_deadline_overflow_halts_before_down() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_tag = binding.tag;
    let delay = reconnect_delay_ns(1, binding.id);
    let at = u64::MAX - delay + 1;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before_records = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(at))
        .expect("queue disconnect");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::TimeOverflow)
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before_records);
    assert_eq!(supervisor.snapshot(binding.id).expect("state").tag, old_tag);
}

#[test]
fn connected_heartbeat_deadline_overflow_halts_before_up_or_subscribe() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    supervisor
        .queue_connected(
            binding.connection_id,
            binding.tag.connection,
            stamp(u64::MAX),
        )
        .expect("queue connected");

    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::TimeOverflow)
    );
    assert!(supervisor.is_halted());
    assert!(sink.frames.is_empty());
    let state = supervisor.snapshot(binding.id).expect("state");
    assert_eq!(state.transport, Transport::Unknown);
    assert_eq!(state.subscription, SubscriptionState::Connecting);
}

#[test]
fn pong_heartbeat_deadline_overflow_halts_before_new_up_record() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before_records = sink.frames.len();

    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(u64::MAX),
            b"pong".to_vec(),
        )
        .expect("queue pong");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::TimeOverflow)
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before_records);
}

#[test]
fn ping_timer_pong_deadline_overflow_halts_before_timer_or_ping() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let connected_at = u64::MAX - HEARTBEAT_INTERVAL_NS;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, connected_at);
    let before_records = sink.frames.len();

    supervisor
        .queue_tick(stamp(u64::MAX))
        .expect("queue ping timer");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::TimeOverflow)
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before_records);
}

#[test]
fn terminal_down_suppresses_queued_connected_without_subscribe_or_up_record() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);

    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(10))
        .expect("queue terminal down");
    supervisor
        .queue_connected(binding.connection_id, old_epoch, stamp(11))
        .expect("queue stale connected");
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(12))
        .expect("queue duplicate down");

    let first = supervisor
        .drain_one(&mut sink)
        .expect("first drain")
        .expect("down result");
    assert_eq!(
        first
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::Close { .. }))
            .count(),
        1
    );

    let obsolete = supervisor
        .drain_one(&mut sink)
        .expect("connected drain")
        .expect("obsolete result");
    assert!(obsolete.commands.is_empty());
    assert!(obsolete.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::ObsoleteControl { stream, epoch }
            if *stream == binding.id && *epoch == old_epoch
    )));
    let pending = supervisor.snapshot(binding.id).expect("pending");
    assert_eq!(pending.transport, Transport::Down);
    assert_eq!(pending.subscription, SubscriptionState::Degraded);

    let duplicate = supervisor
        .drain_one(&mut sink)
        .expect("duplicate down drain")
        .expect("duplicate result");
    assert!(duplicate.commands.is_empty());
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("pending duplicate")
            .tag
            .connection,
        old_epoch
    );
    let final_result = supervisor
        .drain_one(&mut sink)
        .expect("complete duplicate down")
        .expect("completion result");
    assert_eq!(
        final_result
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Up,
                        ..
                    },
                    ..
                })
            ))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ))
            .count(),
        3
    );
    let current = supervisor.snapshot(binding.id).expect("current");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
}

#[test]
fn terminal_timeout_suppresses_queued_pong_without_up_or_heartbeat_revival() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;
    supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
    supervisor
        .drain_one(&mut sink)
        .expect("drain ping")
        .expect("ping result");
    let timeout = ping_due + PONG_TIMEOUT_NS_V1;
    supervisor
        .queue_tick(stamp(timeout))
        .expect("queue timeout");
    supervisor
        .queue_text(
            binding.connection_id,
            old_epoch,
            stamp(timeout + 1),
            b"pong".to_vec(),
        )
        .expect("queue late pong");

    let timed_out = supervisor
        .drain_one(&mut sink)
        .expect("timeout drain")
        .expect("timeout result");
    assert!(
        !timed_out
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor.snapshot(binding.id).expect("pending").transport,
        Transport::Down
    );

    let late = supervisor
        .drain_one(&mut sink)
        .expect("late pong drain")
        .expect("late pong result");
    assert!(late.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::ObsoleteControl { stream, epoch }
            if *stream == binding.id && *epoch == old_epoch
    )));
    assert!(
        !late
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::PongRecorded { .. }))
    );
    assert!(late.commands.is_empty());
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("pending after pong")
            .tag
            .connection,
        old_epoch
    );
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete timeout after pong")
        .expect("completion result");
    assert_eq!(
        completed
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Up,
                        ..
                    },
                    ..
                })
            ))
            .count(),
        1
    );
    let current = supervisor.snapshot(binding.id).expect("current");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
}

#[test]
fn terminal_down_records_queued_ping_timer_without_sending_ping() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(ping_due - 1))
        .expect("queue down");
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("queue ping timer");

    supervisor
        .drain_one(&mut sink)
        .expect("down drain")
        .expect("down result");
    let timer = supervisor
        .drain_one(&mut sink)
        .expect("timer drain")
        .expect("timer result");
    assert!(!timer.commands.iter().any(|command| matches!(
        command,
        TransportCommand::SendText { text, .. } if text == "ping"
    )));
    assert!(
        timer
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::HeartbeatTimerRecorded { .. }))
    );
    assert!(timer.commands.is_empty());
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("pending after timer")
            .tag
            .connection,
        old_epoch
    );
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("complete after timer")
        .expect("completion result");
    assert_eq!(
        completed
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    let current = supervisor.snapshot(binding.id).expect("current");
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
}

#[test]
fn terminal_control_barrier_between_duplicate_disconnects_finishes_once() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_epoch = binding.tag.connection;
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);

    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(ping_due - 2))
        .expect("first down");
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("control barrier");
    supervisor
        .queue_disconnected(binding.connection_id, old_epoch, stamp(ping_due + 1))
        .expect("second down");

    supervisor
        .drain_one(&mut sink)
        .expect("first down drain")
        .expect("first result");
    let timer = supervisor
        .drain_one(&mut sink)
        .expect("timer drain")
        .expect("timer result");
    assert!(
        !timer
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("still pending")
            .transport,
        Transport::Down
    );

    let duplicate = supervisor
        .drain_one(&mut sink)
        .expect("second down drain")
        .expect("duplicate result");
    assert!(duplicate.commands.is_empty());
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("pending duplicate")
            .tag
            .connection,
        old_epoch
    );
    let final_result = supervisor
        .drain_one(&mut sink)
        .expect("complete after second down")
        .expect("completion result");
    assert_eq!(
        final_result
            .commands
            .iter()
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        sink.frames
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ))
            .count(),
        3
    );
}

#[test]
fn terminal_control_of_one_stream_does_not_block_neighbor_operational_control() {
    let a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let mut supervisor = supervisor(
        vec![a.clone(), b.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &a, 1);
    connect_one(&mut supervisor, &mut sink, &b, 2);

    supervisor
        .queue_disconnected(a.connection_id, a.tag.connection, stamp(10))
        .expect("a down");
    supervisor
        .queue_connected(a.connection_id, a.tag.connection, stamp(11))
        .expect("a obsolete connected");
    supervisor
        .queue_text(
            b.connection_id,
            b.tag.connection,
            stamp(12),
            b"pong".to_vec(),
        )
        .expect("b pong");

    supervisor
        .drain_one(&mut sink)
        .expect("a down drain")
        .expect("a down result");
    let obsolete = supervisor
        .drain_one(&mut sink)
        .expect("a obsolete drain")
        .expect("a obsolete result");
    assert!(obsolete.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::ObsoleteControl { stream, .. } if *stream == a.id
    )));
    assert!(obsolete.commands.is_empty());
    assert_eq!(supervisor.snapshot(a.id).expect("a pending").tag, a.tag);
    let a_completed = supervisor
        .drain_one(&mut sink)
        .expect("complete a before neighbor ingress")
        .expect("a completion result");
    assert!(matches!(
        a_completed.commands.as_slice(),
        [TransportCommand::ReconnectAfter { connection, .. }] if *connection == a.connection_id
    ));
    assert_eq!(supervisor.queued_items(), 1);
    let b_pong = supervisor
        .drain_one(&mut sink)
        .expect("b pong drain")
        .expect("b pong result");
    assert!(b_pong.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::PongRecorded { stream, .. } if *stream == b.id
    )));
    let b_state = supervisor.snapshot(b.id).expect("b state");
    assert_eq!(b_state.transport, Transport::Up);
    assert_eq!(b_state.subscription, SubscriptionState::AwaitingAck);
}

#[test]
fn max_connection_epoch_disconnect_halts_before_down() {
    let mut binding = stream_binding(1, 1, 1, "BTCUSDT");
    binding.tag.connection = id(ConnectionEpoch::new(u64::MAX));
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(2))
        .expect("queue down");
    assert!(matches!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::Identity(IdentityError::CounterExhausted(
            "ConnectionEpoch"
        )))
    ));
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before);
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("state")
            .tag
            .connection
            .get(),
        u64::MAX
    );
}

#[test]
fn max_subscription_epoch_disconnect_halts_before_down() {
    let mut binding = stream_binding(1, 1, 1, "BTCUSDT");
    binding.tag.subscription = id(SubscriptionEpoch::new(u64::MAX));
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(2))
        .expect("queue down");
    assert!(matches!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::Identity(IdentityError::CounterExhausted(
            "SubscriptionEpoch"
        )))
    ));
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before);
}

#[test]
fn max_book_epoch_disconnect_halts_before_down() {
    let mut binding = stream_binding(1, 1, 1, "BTCUSDT");
    binding.tag.book = Some(id(BookEpoch::new(u64::MAX)));
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    let before = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(2))
        .expect("queue down");
    assert!(matches!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::Identity(IdentityError::CounterExhausted(
            "BookEpoch"
        )))
    ));
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before);
}

#[test]
fn canonical_owner_rejects_forged_max_record_frontier_before_any_capture_write() {
    // The historical near-MAX disconnect arithmetic regression is retained in
    // private core tests. A public caller cannot assign such a prefix to a fresh
    // canonical writer; the authority now rejects it before capture admission.
    for next in [u64::MAX - 1, u64::MAX - 4] {
        let binding = stream_binding(1, 1, 1, "BTCUSDT");
        let config = supervisor_config_with_record(
            vec![binding],
            QueuePolicy::default(),
            RecordingGate::Written,
            id(RecordNo::new(next)),
        );
        assert!(matches!(
            PublicWsSupervisor::new(config),
            Err(SupervisorError::Authority(
                session::AuthorityError::InvalidBinding
            ))
        ));
    }
}

#[test]
fn accepted_wal_writer_preserves_definition_control_and_raw_order_offline() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = canonical_supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Flushed,
    );
    let mut sink = MemorySink::default();
    let path = supervisor.temp.path.clone();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(101),
            ack("BTCUSDT"),
        )
        .expect("ack");
    supervisor.drain_one(&mut sink).expect("ack drain");
    let raw_snapshot = snapshot("BTCUSDT", "1");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(102),
            raw_snapshot.clone(),
        )
        .expect("snapshot");
    supervisor.drain_one(&mut sink).expect("snapshot drain");
    drop(sink);

    let mut reader = WalReader::open(&path).expect("open wal");
    let mut records = Vec::new();
    while let Some(frame) = reader.next_record().expect("read record") {
        records.push(frame);
    }
    assert_eq!(records.len(), 7);
    assert!(matches!(records[0].value, Record::ArchiveStart(_)));
    assert!(matches!(records[1].value, Record::InstrumentSpec(_)));
    assert!(matches!(records[2].value, Record::StreamDefinition(_)));
    assert!(matches!(records[3].value, Record::ConfigDefinition(_)));
    assert!(matches!(
        records[4].value,
        Record::Control(ControlRecord {
            value: Control::Transport {
                value: Transport::Up,
                ..
            },
            ..
        })
    ));
    assert!(matches!(records[5].value, Record::RawInput(_)));
    match &records[6].value {
        Record::RawInput(raw) => {
            assert_eq!(raw.bytes, raw_snapshot);
            assert_eq!(raw.context.monotonic_ns.get(), 102);
        }
        other => panic!("expected raw snapshot, got {other:?}"),
    }
    assert_eq!(reader.report().last_record.map(RecordNo::get), Some(7));
}

fn artifact() -> ArtifactRef {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("artifact")
}

fn bootstrap_context(n: u64) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(n as i64),
        monotonic_ns: MonotonicNs::new(n),
        context: InputContext::Bootstrap,
    }
}

fn bootstrap_prefix(binding: &StreamBinding, gate: RecordingGate) -> Vec<RecordFrame> {
    let numeric = NumericSpec::new(NumericSpecFields {
        reference: binding.spec.clone(),
        price_units: PriceUnits {
            quote: id(Token::new("USDT")),
            basis: id(Token::new("BTC")),
        },
        quantity_unit: id(Token::new("BTC")),
        base_asset: id(Token::new("BTC")),
        price_increment: ExactDecimal::ONE,
        quantity_increment: ExactDecimal::ONE,
        quantity_to_base_multiplier: Some(ExactDecimal::ONE),
    })
    .expect("synthetic numeric spec");
    let policy = PolicyFields {
        silence_rule: SilenceRule::UnknownOnSilence,
        freshness_deadline_ns: Some(1_000_000_000),
        warmup_min_updates: Some(1),
        warmup_min_elapsed_ns: Some(0),
        allow_quiet_with_proof: false,
        require_two_sided_snapshot: true,
        recording_gate: gate,
    };

    vec![
        RecordFrame {
            record_no: id(RecordNo::new(1)),
            segment_no: SegmentNo::new(0),
            value: Record::ArchiveStart(ArchiveStart {
                archive: id(ArchiveId::new([1; 16])),
                session: id(CaptureSessionId::new([2; 16])),
                clock: id(ClockId::new(1)),
                mode: DurabilityMode::Buffered,
                previous_archive: None,
            }),
        },
        RecordFrame {
            record_no: id(RecordNo::new(2)),
            segment_no: SegmentNo::new(0),
            value: Record::InstrumentSpec(InstrumentSpecRecord {
                context: bootstrap_context(2),
                slot: binding.instrument_slot,
                numeric,
                provenance: artifact(),
            }),
        },
        RecordFrame {
            record_no: id(RecordNo::new(3)),
            segment_no: SegmentNo::new(0),
            value: Record::StreamDefinition(StreamDefinition {
                context: bootstrap_context(3),
                binding: binding.clone(),
                provenance: artifact(),
            }),
        },
        RecordFrame {
            record_no: id(RecordNo::new(4)),
            segment_no: SegmentNo::new(0),
            value: Record::ConfigDefinition(ConfigDefinition {
                context: bootstrap_context(4),
                next: active_context(),
                provenance_kind: ProvenanceKind::Synthetic,
                evidence: artifact(),
                fields: policy,
            }),
        },
    ]
}

fn bootstrap_prefix_many(bindings: &[StreamBinding], gate: RecordingGate) -> Vec<RecordFrame> {
    let mut out = Vec::new();
    let first = bootstrap_prefix(bindings.first().expect("configured stream"), gate);
    out.push(first[0].clone());
    for binding in bindings {
        let prefix = bootstrap_prefix(binding, gate);
        for mut frame in [prefix[1].clone(), prefix[2].clone()] {
            let next = out.len() as u64 + 1;
            frame.record_no = id(RecordNo::new(next));
            match &mut frame.value {
                Record::InstrumentSpec(spec) => spec.context = bootstrap_context(next),
                Record::StreamDefinition(stream) => stream.context = bootstrap_context(next),
                _ => unreachable!(),
            }
            out.push(frame);
        }
    }
    let mut config = first[3].clone();
    let next = out.len() as u64 + 1;
    config.record_no = id(RecordNo::new(next));
    let Record::ConfigDefinition(definition) = &mut config.value else {
        unreachable!()
    };
    definition.context = bootstrap_context(next);
    out.push(config);
    out
}

#[derive(Clone, Copy, Debug)]
enum EpochPersistenceFault {
    Persist,
    ReceiptMismatch,
    InsufficientGate,
}

struct EpochFaultSink {
    fault_at: usize,
    fault: EpochPersistenceFault,
    epoch_attempts: usize,
    attempts: Vec<(RecordFrame, RecordingGate)>,
    confirmed: Vec<RecordFrame>,
}

impl EpochFaultSink {
    fn new(fault_at: usize, fault: EpochPersistenceFault) -> Self {
        Self {
            fault_at,
            fault,
            epoch_attempts: 0,
            attempts: Vec::new(),
            confirmed: Vec::new(),
        }
    }
}

impl RecordSink for EpochFaultSink {
    fn fault_plan(&self) -> FaultPlan {
        FaultPlan::Epoch {
            fault_at: self.fault_at,
            seen: self.epoch_attempts,
            fault: self.fault,
        }
    }
    fn observe(&mut self, attempts: Vec<AttemptTrace>) {
        for attempt in attempts {
            if matches!(
                &attempt.frame.value,
                Record::Control(ControlRecord {
                    value: Control::EpochAdvance { .. },
                    ..
                })
            ) {
                self.epoch_attempts += 1;
            }
            if attempt.confirmed {
                self.confirmed.push(attempt.frame.clone());
            }
            self.attempts.push((attempt.frame, attempt.gate));
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum DeferredDisconnectInput {
    Immediate,
    PongTimeout,
    RawBarrier,
    QueueOverflowBarrier,
    TimerBarrier,
    DuplicateBarrier,
}

fn failed_archive_snapshot(
    mut previous: StreamSupervisorSnapshot,
    supervisor: &PublicWsSupervisor,
) -> StreamSupervisorSnapshot {
    let disposition = supervisor.authority.disposition();
    assert!(matches!(
        disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    // The only intended snapshot delta after a failed gated write is the
    // irreversible archive disposition. Every protocol/frontier field remains
    // subject to the historical full equality assertion.
    previous.session_disposition = Some(disposition);
    previous
}

fn assert_epoch_fault(error: SupervisorError, sink: &EpochFaultSink) {
    let failed = &sink.attempts.last().expect("failed persistence attempt").0;
    assert_eq!(
        error,
        match sink.fault {
            EpochPersistenceFault::Persist =>
                SupervisorError::Persistence(PersistError::new("injected epoch persistence error",)),
            EpochPersistenceFault::ReceiptMismatch => SupervisorError::PersistenceReceiptMismatch {
                expected: failed.record_no,
                actual: failed.record_no.checked_next().expect("mismatched receipt"),
            },
            EpochPersistenceFault::InsufficientGate => SupervisorError::PersistenceGateTooWeak {
                required: RecordingGate::Durable,
                achieved: RecordingGate::Written,
            },
        }
    );
    assert!(
        !sink
            .confirmed
            .iter()
            .any(|frame| frame.record_no == failed.record_no)
    );
}

fn exercise_deferred_epoch_fault(
    input: DeferredDisconnectInput,
    fault_at: usize,
    fault: EpochPersistenceFault,
) {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_tag = binding.tag;
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 8,
    };
    let mut supervisor = supervisor(vec![binding.clone()], queue, RecordingGate::Durable);
    let mut sink = EpochFaultSink::new(fault_at, fault);
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);
    let ping_due = 100 + HEARTBEAT_INTERVAL_NS;

    if matches!(input, DeferredDisconnectInput::PongTimeout) {
        supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
        let ping = supervisor
            .drain_one(&mut sink)
            .expect("ping drain")
            .expect("ping");
        assert!(
            matches!(ping.commands.as_slice(), [TransportCommand::SendText { text, .. }] if text == "ping")
        );
        supervisor
            .queue_tick(stamp(ping_due + PONG_TIMEOUT_NS_V1))
            .expect("queue timeout");
    } else {
        let down_at = if matches!(input, DeferredDisconnectInput::TimerBarrier) {
            ping_due - 1
        } else {
            200
        };
        supervisor
            .queue_disconnected(binding.connection_id, old_tag.connection, stamp(down_at))
            .expect("queue disconnect");
    }
    match input {
        DeferredDisconnectInput::RawBarrier | DeferredDisconnectInput::QueueOverflowBarrier => {
            supervisor
                .queue_text(
                    binding.connection_id,
                    old_tag.connection,
                    stamp(201),
                    snapshot("BTCUSDT", "7"),
                )
                .expect("queue old raw");
            if matches!(input, DeferredDisconnectInput::QueueOverflowBarrier) {
                supervisor
                    .queue_text(
                        binding.connection_id,
                        old_tag.connection,
                        stamp(202),
                        update("BTCUSDT", 10, 11),
                    )
                    .expect("queue loss provenance");
            }
        }
        DeferredDisconnectInput::TimerBarrier => supervisor
            .queue_tick(stamp(ping_due))
            .expect("queue timer barrier"),
        DeferredDisconnectInput::DuplicateBarrier => supervisor
            .queue_disconnected(binding.connection_id, old_tag.connection, stamp(201))
            .expect("queue duplicate"),
        DeferredDisconnectInput::Immediate | DeferredDisconnectInput::PongTimeout => {}
    }

    let down = supervisor
        .drain_one(&mut sink)
        .expect("Down must return before completion")
        .expect("Down result");
    assert_eq!(
        down.commands,
        vec![TransportCommand::Close {
            connection: binding.connection_id,
            epoch: old_tag.connection,
        }]
    );
    assert!(
        !down
            .events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::EpochAdvanced { .. }))
    );
    assert_eq!(
        sink.epoch_attempts, 0,
        "no fallible completion before caller receives Close"
    );
    let down_record = sink
        .confirmed
        .iter()
        .find(|frame| {
            matches!(&frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport { connection, epoch, value: Transport::Down },
                    ..
                }) if *connection == binding.connection_id && *epoch == old_tag.connection
            )
        })
        .expect("terminal Down confirmed at Durable gate");
    assert!(down.records.contains(&down_record.record_no));
    assert!(
        sink.attempts
            .iter()
            .all(|(_, gate)| *gate == RecordingGate::Durable)
    );
    if matches!(input, DeferredDisconnectInput::PongTimeout) {
        assert_eq!(down.records.len(), 2);
        assert!(
            down.events
                .iter()
                .any(|event| matches!(event, SupervisorEvent::HeartbeatTimerRecorded { .. }))
        );
    }
    let terminal = supervisor
        .snapshot(binding.id)
        .expect("terminal generation");
    assert_eq!(terminal.tag, old_tag);
    assert_eq!(terminal.transport, Transport::Down);
    assert_eq!(terminal.subscription, SubscriptionState::Degraded);

    while supervisor.queued_items() != 0 {
        let barrier = supervisor
            .drain_one(&mut sink)
            .expect("barrier persistence")
            .expect("barrier result");
        assert!(
            barrier.commands.is_empty(),
            "terminal barrier cannot revive or duplicate Close"
        );
        assert_eq!(
            sink.epoch_attempts, 0,
            "already admitted provenance blocks epoch completion"
        );
        assert_eq!(
            supervisor.snapshot(binding.id).expect("old scope").tag,
            old_tag
        );
    }
    match input {
        DeferredDisconnectInput::RawBarrier | DeferredDisconnectInput::QueueOverflowBarrier => {
            assert!(sink.confirmed.iter().any(|frame| matches!(&frame.value, Record::RawInput(raw)
                if raw.tag == old_tag && raw.bytes == snapshot("BTCUSDT", "7") && raw.attempt.get() == 1)));
            if matches!(input, DeferredDisconnectInput::QueueOverflowBarrier) {
                assert!(
                    sink.confirmed
                        .iter()
                        .any(|frame| matches!(&frame.value, Record::Gap(gap)
                    if gap.reason == Reason::QueueOverflow))
                );
            }
        }
        DeferredDisconnectInput::TimerBarrier => {
            assert!(sink.confirmed.iter().any(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Timer { .. },
                    ..
                })
            )))
        }
        DeferredDisconnectInput::DuplicateBarrier => assert_eq!(
            sink.confirmed
                .iter()
                .filter(|frame| matches!(
                    &frame.value,
                    Record::Control(ControlRecord {
                        value: Control::Transport {
                            value: Transport::Down,
                            ..
                        },
                        ..
                    })
                ))
                .count(),
            2
        ),
        DeferredDisconnectInput::Immediate | DeferredDisconnectInput::PongTimeout => {}
    }

    let terminal = supervisor
        .snapshot(binding.id)
        .expect("terminal after admitted barriers");
    let prefix_len = sink.confirmed.len();
    let error = supervisor
        .drain_one(&mut sink)
        .expect_err("next drain reports original completion failure");
    assert_epoch_fault(error, &sink);
    let terminal = failed_archive_snapshot(terminal, &supervisor);
    assert!(supervisor.is_halted());
    assert_eq!(sink.epoch_attempts, fault_at);
    assert_eq!(sink.confirmed.len(), prefix_len + fault_at - 1);
    let expected_changes: Vec<_> = sink
        .attempts
        .iter()
        .filter_map(|(frame, _)| match &frame.value {
            Record::Control(ControlRecord {
                value: Control::EpochAdvance { change, .. },
                ..
            }) => Some(change),
            _ => None,
        })
        .collect();
    assert!(matches!(
        expected_changes[0],
        EpochChange::Connection { .. }
    ));
    if fault_at >= 2 {
        assert!(matches!(
            expected_changes[1],
            EpochChange::Subscription { .. }
        ));
    }
    if fault_at == 3 {
        assert!(matches!(expected_changes[2], EpochChange::Book { .. }));
    }
    assert!(sink.confirmed.windows(2).all(|pair| {
        pair[0].record_no.checked_next().expect("prefix number") == pair[1].record_no
    }));
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("runtime remains atomic"),
        terminal
    );
    let attempts_after_failure = sink.attempts.len();
    for _ in 0..2 {
        assert_eq!(
            supervisor.drain_one(&mut sink),
            Err(SupervisorError::Halted)
        );
        assert_eq!(
            supervisor.queue_connected(binding.connection_id, old_tag.connection, stamp(300)),
            Err(SupervisorError::Halted)
        );
        assert_eq!(
            supervisor.queue_disconnected(binding.connection_id, old_tag.connection, stamp(301)),
            Err(SupervisorError::Halted)
        );
        assert_eq!(
            supervisor.queue_text(
                binding.connection_id,
                old_tag.connection,
                stamp(302),
                snapshot("BTCUSDT", "9"),
            ),
            Err(SupervisorError::Halted)
        );
        assert_eq!(
            supervisor.queue_tick(stamp(ping_due + PONG_TIMEOUT_NS_V1 + 1)),
            Err(SupervisorError::Halted)
        );
    }
    assert_eq!(
        sink.attempts.len(),
        attempts_after_failure,
        "no persistence retry after terminal failure"
    );
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("no old-generation revival"),
        terminal
    );
}

#[test]
fn disconnected_close_survives_every_epoch_completion_storage_failure() {
    for fault_at in 1..=3 {
        for fault in [
            EpochPersistenceFault::Persist,
            EpochPersistenceFault::ReceiptMismatch,
            EpochPersistenceFault::InsufficientGate,
        ] {
            exercise_deferred_epoch_fault(DeferredDisconnectInput::Immediate, fault_at, fault);
        }
    }
}

#[test]
fn pong_timeout_close_survives_every_epoch_completion_storage_failure() {
    for fault_at in 1..=3 {
        for fault in [
            EpochPersistenceFault::Persist,
            EpochPersistenceFault::ReceiptMismatch,
            EpochPersistenceFault::InsufficientGate,
        ] {
            exercise_deferred_epoch_fault(DeferredDisconnectInput::PongTimeout, fault_at, fault);
        }
    }
}

#[test]
fn close_survives_epoch_completion_faults_after_terminal_ingress_barriers() {
    for input in [
        DeferredDisconnectInput::RawBarrier,
        DeferredDisconnectInput::QueueOverflowBarrier,
        DeferredDisconnectInput::TimerBarrier,
        DeferredDisconnectInput::DuplicateBarrier,
    ] {
        for fault_at in 1..=3 {
            for fault in [
                EpochPersistenceFault::Persist,
                EpochPersistenceFault::ReceiptMismatch,
                EpochPersistenceFault::InsufficientGate,
            ] {
                exercise_deferred_epoch_fault(input, fault_at, fault);
            }
        }
    }
}

#[test]
fn empty_queue_deferred_completion_emits_one_reconnect_after_duplicate_and_barriers() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let old_tag = binding.tag;
    let queue = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 8,
    };
    let mut supervisor = supervisor(vec![binding.clone()], queue, RecordingGate::Durable);
    let mut sink = EpochFaultSink::new(usize::MAX, EpochPersistenceFault::Persist);
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 100);
    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(200))
        .expect("disconnect");
    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(201),
            snapshot("BTCUSDT", "7"),
        )
        .expect("raw barrier");
    supervisor
        .queue_text(
            binding.connection_id,
            old_tag.connection,
            stamp(202),
            update("BTCUSDT", 10, 11),
        )
        .expect("loss barrier");
    supervisor
        .queue_connected(binding.connection_id, old_tag.connection, stamp(203))
        .expect("obsolete control barrier");
    supervisor
        .queue_disconnected(binding.connection_id, old_tag.connection, stamp(204))
        .expect("duplicate barrier");
    let mut returned = Vec::new();
    while supervisor.queued_items() != 0 {
        returned.push(
            supervisor
                .drain_one(&mut sink)
                .expect("persist ingress")
                .expect("ingress result"),
        );
        assert_eq!(
            supervisor
                .snapshot(binding.id)
                .expect("pending old scope")
                .tag,
            old_tag
        );
        assert_eq!(sink.epoch_attempts, 0);
    }
    assert_eq!(
        returned
            .iter()
            .flat_map(|result| &result.commands)
            .filter(|command| matches!(command, TransportCommand::Close { .. }))
            .count(),
        1
    );
    assert!(
        returned
            .iter()
            .flat_map(|result| &result.commands)
            .all(|command| matches!(command, TransportCommand::Close { .. }))
    );
    assert!(
        returned
            .iter()
            .flat_map(|result| &result.events)
            .any(|event| matches!(event, SupervisorEvent::ObsoleteControl { .. }))
    );
    let completion = supervisor
        .drain_one(&mut sink)
        .expect("empty queue completion")
        .expect("completion result");
    assert_eq!(completion.records.len(), 3);
    assert!(
        matches!(completion.commands.as_slice(), [TransportCommand::ReconnectAfter { connection, epoch, .. }]
        if *connection == binding.connection_id && epoch.get() == 2)
    );
    assert!(
        matches!(completion.events.as_slice(), [SupervisorEvent::EpochAdvanced { tag, .. }]
        if tag.connection.get() == 2 && tag.subscription.get() == 2 && tag.book.expect("book").get() == 2)
    );
    assert_eq!(sink.epoch_attempts, 3);
    let current = supervisor.snapshot(binding.id).expect("new scope");
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(current.capture_attempt_frontier, 2);
    let attempts = sink.attempts.len();
    assert_eq!(supervisor.drain_one(&mut sink), Ok(None));
    assert_eq!(sink.attempts.len(), attempts);
    assert_eq!(
        supervisor.queue_disconnected(binding.connection_id, old_tag.connection, stamp(205)),
        Err(SupervisorError::UnknownConnectionEpoch {
            connection: binding.connection_id,
            epoch: old_tag.connection
        })
    );
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("stale does not advance")
            .tag,
        current.tag
    );
}

#[test]
fn neighbor_commands_are_returned_before_failing_other_stream_completion() {
    for fault_at in 1..=3 {
        for fault in [
            EpochPersistenceFault::Persist,
            EpochPersistenceFault::ReceiptMismatch,
            EpochPersistenceFault::InsufficientGate,
        ] {
            let a = stream_binding(1, 1, 1, "BTCUSDT");
            let b = stream_binding(2, 2, 2, "ETHUSDT");
            let mut supervisor = supervisor(
                vec![a.clone(), b.clone()],
                QueuePolicy::default(),
                RecordingGate::Durable,
            );
            let mut sink = EpochFaultSink::new(fault_at, fault);
            supervisor.start_commands().expect("start");
            supervisor
                .queue_disconnected(a.connection_id, a.tag.connection, stamp(1))
                .expect("A Down");
            supervisor
                .queue_connected(b.connection_id, b.tag.connection, stamp(2))
                .expect("B connected");
            supervisor
                .queue_connected(a.connection_id, a.tag.connection, stamp(3))
                .expect("first A barrier");
            let a_down = supervisor
                .drain_one(&mut sink)
                .expect("A Down drain")
                .expect("A Down result");
            assert_eq!(
                a_down.commands,
                vec![TransportCommand::Close {
                    connection: a.connection_id,
                    epoch: a.tag.connection
                }]
            );
            let b_subscribe = supervisor
                .drain_one(&mut sink)
                .expect("B connected drain")
                .expect("B subscription command");
            assert!(
                matches!(b_subscribe.commands.as_slice(), [TransportCommand::SendText { connection, text, .. }]
                if *connection == b.connection_id && text.contains("subscribe"))
            );
            let ping_due = 2 + HEARTBEAT_INTERVAL_NS;
            supervisor
                .queue_tick(stamp(ping_due))
                .expect("queue B ping");
            supervisor
                .queue_connected(a.connection_id, a.tag.connection, stamp(ping_due + 1))
                .expect("last A barrier");
            let first_barrier = supervisor
                .drain_one(&mut sink)
                .expect("first A barrier drain")
                .expect("first A barrier");
            assert!(first_barrier.commands.is_empty());
            let b_ping = supervisor
                .drain_one(&mut sink)
                .expect("B timer drain")
                .expect("B ping result");
            assert!(
                matches!(b_ping.commands.as_slice(), [TransportCommand::SendText { connection, text, .. }]
                if *connection == b.connection_id && text == "ping")
            );
            supervisor
                .queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(ping_due + 2),
                    b"pong".to_vec(),
                )
                .expect("future B ingress");
            supervisor
                .drain_one(&mut sink)
                .expect("last A barrier drain")
                .expect("last A barrier");
            assert_eq!(supervisor.queued_items(), 1);
            let b_before_failure = supervisor.snapshot(b.id).expect("B before failure");
            let error = supervisor
                .drain_one(&mut sink)
                .expect_err("A completion must fail before consuming B ingress");
            assert_epoch_fault(error, &sink);
            let b_before_failure = failed_archive_snapshot(b_before_failure, &supervisor);
            assert!(supervisor.is_halted());
            assert_eq!(
                supervisor.queued_items(),
                1,
                "B ingress remains unconsumed after terminal A failure"
            );
            assert_eq!(
                supervisor.snapshot(b.id).expect("B after failure"),
                b_before_failure
            );
            assert_eq!(
                b_subscribe.commands.len(),
                1,
                "already returned B subscription remains available"
            );
            assert_eq!(
                b_ping.commands.len(),
                1,
                "already returned B ping remains available"
            );
            assert_eq!(
                supervisor.snapshot(a.id).expect("atomic A scope").tag,
                a.tag
            );
            let attempts = sink.attempts.len();
            assert_eq!(
                supervisor.drain_one(&mut sink),
                Err(SupervisorError::Halted)
            );
            assert_eq!(sink.attempts.len(), attempts);
        }
    }
}

fn q2_ping_count(results: &[DrainResult]) -> usize {
    results
        .iter()
        .flat_map(|result| &result.commands)
        .filter(|command| {
            matches!(
                command,
                TransportCommand::SendText { text, .. } if text == "ping"
            )
        })
        .count()
}

fn q2_assert_timer_only(result: &DrainResult) {
    assert_eq!(result.records.len(), 1);
    assert!(result.commands.is_empty());
    assert!(matches!(
        result.events.as_slice(),
        [SupervisorEvent::HeartbeatTimerRecorded { .. }]
    ));
}

fn q2_assert_no_terminal_records(frames: &[RecordFrame]) {
    assert!(!frames.iter().any(|frame| matches!(
        &frame.value,
        Record::Control(ControlRecord {
            value: Control::Transport {
                value: Transport::Down,
                ..
            } | Control::EpochAdvance { .. },
            ..
        })
    )));
}

fn q2_connected_fixture(binding: &StreamBinding) -> (PublicWsSupervisor, MemorySink, u64) {
    let mut supervisor = supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Durable,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, binding, 100);
    (supervisor, sink, 100 + HEARTBEAT_INTERVAL_NS)
}

fn q2_send_initial_ping(
    supervisor: &mut PublicWsSupervisor,
    sink: &mut impl RecordSink,
    ping_due: u64,
) -> u64 {
    supervisor.queue_tick(stamp(ping_due)).expect("queue ping");
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("repeat ping tick");
    assert_eq!(supervisor.queued_items(), 1);
    let ping = supervisor
        .drain_one(sink)
        .expect("drain ping")
        .expect("ping outcome");
    assert_eq!(q2_ping_count(&[ping]), 1);
    ping_due + PONG_TIMEOUT_NS_V1
}

#[test]
fn pong_before_queued_timeout_preserves_heartbeat_at_deadline_boundaries() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let deadline = 100 + HEARTBEAT_INTERVAL_NS + PONG_TIMEOUT_NS_V1;
    for pong_at in [deadline - 1, deadline, deadline + 1] {
        let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
        assert_eq!(
            q2_send_initial_ping(&mut supervisor, &mut sink, ping_due),
            deadline
        );
        let prefix_len = sink.frames.len();
        supervisor
            .queue_text(
                binding.connection_id,
                binding.tag.connection,
                stamp(pong_at),
                b"pong".to_vec(),
            )
            .expect("queue Pong before timer observation");
        supervisor
            .queue_tick(stamp(deadline.max(pong_at)))
            .expect("queue timeout behind Pong");
        supervisor
            .queue_tick(stamp(deadline.max(pong_at)))
            .expect("repeated timeout tick");
        assert_eq!(supervisor.queued_items(), 2);

        // Local policy follows accepted FIFO observations: Pong before Timer
        // replaces the schedule at D-1, D and D+1. This is no exchange guarantee.
        let pong = supervisor
            .drain_one(&mut sink)
            .expect("drain Pong")
            .expect("Pong outcome");
        assert!(pong.commands.is_empty());
        assert!(
            pong.events
                .iter()
                .any(|event| matches!(event, SupervisorEvent::PongRecorded { .. }))
        );
        let timer = supervisor
            .drain_one(&mut sink)
            .expect("drain canceled timeout")
            .expect("timer outcome");
        q2_assert_timer_only(&timer);
        q2_assert_no_terminal_records(&sink.frames[prefix_len..]);
        assert!(!supervisor.is_halted());
        let current = supervisor.snapshot(binding.id).expect("current state");
        assert_eq!(current.tag, binding.tag);
        assert_eq!(current.transport, Transport::Up);
        assert!(
            supervisor
                .drain_one(&mut sink)
                .expect("empty queue")
                .is_none()
        );

        let next_ping = pong_at + HEARTBEAT_INTERVAL_NS;
        supervisor
            .queue_tick(stamp(next_ping - 1))
            .expect("before next heartbeat");
        assert_eq!(supervisor.queued_items(), 0);
        supervisor
            .queue_tick(stamp(next_ping))
            .expect("next heartbeat");
        supervisor
            .queue_tick(stamp(next_ping + 1))
            .expect("repeated next heartbeat");
        assert_eq!(supervisor.queued_items(), 1);
        assert_eq!(q2_ping_count(&drain_all(&mut supervisor, &mut sink)), 1);
        q2_assert_no_terminal_records(&sink.frames[prefix_len..]);
    }
}

#[test]
fn pong_cancels_old_ping_timer_without_clearing_equal_deadline_new_owner() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(100),
            b"pong".to_vec(),
        )
        .expect("queue Pong recreating the same deadline");
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("old ping timer");
    supervisor
        .drain_one(&mut sink)
        .expect("Pong accepted")
        .expect("Pong outcome");
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("new ping owner");
    supervisor.queue_tick(stamp(ping_due)).expect("repeat tick");
    assert_eq!(supervisor.queued_items(), 2);

    let old = supervisor
        .drain_one(&mut sink)
        .expect("old timer")
        .expect("old timer outcome");
    q2_assert_timer_only(&old);
    assert!(matches!(
        old.events.as_slice(),
        [SupervisorEvent::HeartbeatTimerRecorded { timer_id: 1, .. }]
    ));
    supervisor
        .queue_tick(stamp(ping_due))
        .expect("owner remains queued");
    assert_eq!(supervisor.queued_items(), 1);
    let active = supervisor
        .drain_one(&mut sink)
        .expect("active timer")
        .expect("active timer outcome");
    assert!(matches!(
        active.events.as_slice(),
        [SupervisorEvent::HeartbeatTimerRecorded { timer_id: 2, .. }]
    ));
    assert_eq!(q2_ping_count(&[active]), 1);
    assert!(!supervisor.is_halted());
    assert_eq!(
        supervisor.snapshot(binding.id).expect("Up").tag,
        binding.tag
    );
    supervisor
        .queue_tick(stamp(ping_due + PONG_TIMEOUT_NS_V1 - 1))
        .expect("before actual pong deadline");
    assert_eq!(supervisor.queued_items(), 0);
    supervisor
        .queue_tick(stamp(ping_due + PONG_TIMEOUT_NS_V1))
        .expect("actual timeout");
    let down = supervisor
        .drain_one(&mut sink)
        .expect("active timeout")
        .expect("terminal outcome");
    assert_eq!(
        down.commands,
        vec![TransportCommand::Close {
            connection: binding.connection_id,
            epoch: binding.tag.connection,
        }]
    );
    let completion = supervisor
        .drain_one(&mut sink)
        .expect("completion")
        .expect("completion outcome");
    assert_eq!(completion.commands.len(), 1);
    assert!(matches!(
        completion.commands[0],
        TransportCommand::ReconnectAfter { .. }
    ));
}

#[test]
fn canceled_timeout_does_not_change_new_heartbeat_cycle_or_queued_owner() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
    let old_deadline = q2_send_initial_ping(&mut supervisor, &mut sink, ping_due);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(old_deadline - 1),
            b"pong".to_vec(),
        )
        .expect("cancel old outstanding cycle");
    supervisor
        .queue_tick(stamp(old_deadline))
        .expect("queue old timeout");
    supervisor
        .drain_one(&mut sink)
        .expect("accept Pong")
        .expect("Pong outcome");
    let new_due = old_deadline - 1 + HEARTBEAT_INTERVAL_NS;
    supervisor
        .queue_tick(stamp(new_due))
        .expect("queue new heartbeat");
    assert_eq!(supervisor.queued_items(), 2);
    let old = supervisor
        .drain_one(&mut sink)
        .expect("obsolete timeout")
        .expect("timer outcome");
    q2_assert_timer_only(&old);
    supervisor
        .queue_tick(stamp(new_due))
        .expect("new owner remains queued");
    assert_eq!(supervisor.queued_items(), 1);
    let ping = supervisor
        .drain_one(&mut sink)
        .expect("new heartbeat")
        .expect("ping outcome");
    assert_eq!(q2_ping_count(&[ping]), 1);
    let new_deadline = new_due + PONG_TIMEOUT_NS_V1;
    supervisor
        .queue_tick(stamp(new_deadline - 1))
        .expect("new deadline not due");
    assert_eq!(supervisor.queued_items(), 0);
    supervisor
        .queue_tick(stamp(new_deadline))
        .expect("new timeout");
    supervisor
        .queue_tick(stamp(new_deadline + 1))
        .expect("repeat new timeout");
    assert_eq!(supervisor.queued_items(), 1);
    let results = drain_all(&mut supervisor, &mut sink);
    assert_eq!(
        results
            .iter()
            .flat_map(|result| &result.commands)
            .filter(|command| matches!(command, TransportCommand::Close { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .flat_map(|result| &result.commands)
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("advanced once")
            .tag
            .connection
            .get(),
        2
    );
}

#[test]
fn obsolete_timers_skip_impossible_operational_time_and_epoch_preflight() {
    for timeout in [false, true] {
        let mut binding = stream_binding(1, 1, 1, "BTCUSDT");
        binding.tag.connection = id(ConnectionEpoch::new(u64::MAX));
        binding.tag.subscription = id(SubscriptionEpoch::new(u64::MAX));
        binding.tag.book = Some(id(BookEpoch::new(u64::MAX)));
        let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
        let pong_at = if timeout {
            q2_send_initial_ping(&mut supervisor, &mut sink, ping_due) - 1
        } else {
            ping_due - 1
        };
        supervisor
            .queue_text(
                binding.connection_id,
                binding.tag.connection,
                stamp(pong_at),
                b"pong".to_vec(),
            )
            .expect("queue schedule replacement");
        supervisor
            .queue_tick(stamp(u64::MAX))
            .expect("queue old timer at MAX");
        supervisor
            .drain_one(&mut sink)
            .expect("Pong replaces owner")
            .expect("Pong outcome");
        let timer = supervisor
            .drain_one(&mut sink)
            .expect("obsolete timer has no operational preflight")
            .expect("timer outcome");
        q2_assert_timer_only(&timer);
        assert!(!supervisor.is_halted());
        let current = supervisor.snapshot(binding.id).expect("current");
        assert_eq!(current.tag, binding.tag);
        assert_eq!(current.transport, Transport::Up);
        q2_assert_no_terminal_records(&sink.frames);
        supervisor
            .queue_tick(stamp(pong_at + HEARTBEAT_INTERVAL_NS - 1))
            .expect("new heartbeat not due");
        assert_eq!(supervisor.queued_items(), 0);
    }
}

#[test]
fn timer_cancellation_of_one_stream_preserves_neighbor_timeout() {
    let a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let mut supervisor = supervisor(
        vec![a.clone(), b.clone()],
        QueuePolicy::default(),
        RecordingGate::Durable,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &a, 100);
    connect_one(&mut supervisor, &mut sink, &b, 200);
    let due = 200 + HEARTBEAT_INTERVAL_NS;
    supervisor.queue_tick(stamp(due)).expect("both pings due");
    assert_eq!(q2_ping_count(&drain_all(&mut supervisor, &mut sink)), 2);
    let deadline = due + PONG_TIMEOUT_NS_V1;
    supervisor
        .queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(deadline - 1),
            b"pong".to_vec(),
        )
        .expect("A Pong");
    supervisor
        .queue_tick(stamp(deadline))
        .expect("both timeout observations");
    supervisor
        .queue_tick(stamp(deadline + 1))
        .expect("repeat timeout tick");
    assert_eq!(supervisor.queued_items(), 3);
    supervisor
        .drain_one(&mut sink)
        .expect("A Pong drain")
        .expect("Pong outcome");
    let obsolete_a = supervisor
        .drain_one(&mut sink)
        .expect("A canceled timeout")
        .expect("timer outcome");
    q2_assert_timer_only(&obsolete_a);
    let b_down = supervisor
        .drain_one(&mut sink)
        .expect("B active timeout")
        .expect("B terminal outcome");
    assert_eq!(
        b_down.commands,
        vec![TransportCommand::Close {
            connection: b.connection_id,
            epoch: b.tag.connection
        }]
    );
    let b_completion = supervisor
        .drain_one(&mut sink)
        .expect("B completion")
        .expect("B completion outcome");
    assert!(
        matches!(b_completion.commands.as_slice(), [TransportCommand::ReconnectAfter { connection, .. }] if *connection == b.connection_id)
    );
    let a_now = supervisor.snapshot(a.id).expect("A remains operational");
    assert_eq!(a_now.tag, a.tag);
    assert_eq!(a_now.transport, Transport::Up);
    assert_eq!(
        supervisor
            .snapshot(b.id)
            .expect("B advanced")
            .tag
            .connection
            .get(),
        2
    );
    supervisor
        .queue_tick(stamp(deadline - 1 + HEARTBEAT_INTERVAL_NS))
        .expect("A next heartbeat");
    let a_ping = supervisor
        .drain_one(&mut sink)
        .expect("A ping")
        .expect("A outcome");
    assert!(
        matches!(a_ping.commands.as_slice(), [TransportCommand::SendText { connection, text, .. }] if *connection == a.connection_id && text == "ping")
    );
}

#[test]
fn timeout_before_queued_pong_remains_terminal_under_fifo_policy() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let deadline = 100 + HEARTBEAT_INTERVAL_NS + PONG_TIMEOUT_NS_V1;
    for pong_at in [deadline - 1, deadline, deadline + 1] {
        let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
        q2_send_initial_ping(&mut supervisor, &mut sink, ping_due);
        supervisor
            .queue_tick(stamp(deadline))
            .expect("timeout first");
        supervisor
            .queue_text(
                binding.connection_id,
                binding.tag.connection,
                stamp(pong_at),
                b"pong".to_vec(),
            )
            .expect("Pong admitted later");
        let down = supervisor
            .drain_one(&mut sink)
            .expect("timeout drain")
            .expect("Down outcome");
        assert_eq!(
            down.commands,
            vec![TransportCommand::Close {
                connection: binding.connection_id,
                epoch: binding.tag.connection
            }]
        );
        let pong = supervisor
            .drain_one(&mut sink)
            .expect("obsolete Pong")
            .expect("Pong diagnostic");
        assert!(pong.records.is_empty());
        assert!(pong.commands.is_empty());
        assert!(matches!(
            pong.events.as_slice(),
            [SupervisorEvent::ObsoleteControl { .. }]
        ));
        let completion = supervisor
            .drain_one(&mut sink)
            .expect("completion")
            .expect("completion outcome");
        assert!(matches!(
            completion.commands.as_slice(),
            [TransportCommand::ReconnectAfter { .. }]
        ));
        assert_eq!(
            supervisor
                .snapshot(binding.id)
                .expect("advanced")
                .tag
                .connection
                .get(),
            2
        );
        assert!(supervisor.drain_one(&mut sink).expect("empty").is_none());
    }
}

#[test]
fn connected_schedule_replacement_cancels_already_queued_ping_timer() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let (mut supervisor, mut sink, old_due) = q2_connected_fixture(&binding);
    supervisor
        .queue_connected(
            binding.connection_id,
            binding.tag.connection,
            stamp(old_due - 1),
        )
        .expect("queued Connected");
    supervisor
        .queue_tick(stamp(old_due))
        .expect("old ping schedule");
    let connected = supervisor
        .drain_one(&mut sink)
        .expect("Connected replacement")
        .expect("Connected outcome");
    assert_eq!(connected.commands.len(), 1);
    let obsolete = supervisor
        .drain_one(&mut sink)
        .expect("obsolete timer")
        .expect("timer outcome");
    q2_assert_timer_only(&obsolete);
    let new_due = old_due - 1 + HEARTBEAT_INTERVAL_NS;
    supervisor
        .queue_tick(stamp(new_due - 1))
        .expect("before replaced due");
    assert_eq!(supervisor.queued_items(), 0);
    supervisor.queue_tick(stamp(new_due)).expect("replaced due");
    assert_eq!(q2_ping_count(&drain_all(&mut supervisor, &mut sink)), 1);
}

struct Q2TimerFaultSink {
    fault: EpochPersistenceFault,
    attempts: Vec<RecordFrame>,
    confirmed: Vec<RecordFrame>,
}

#[test]
fn disconnect_cancels_queued_timeout_without_duplicate_down_or_close() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let (mut supervisor, mut sink, ping_due) = q2_connected_fixture(&binding);
    let deadline = q2_send_initial_ping(&mut supervisor, &mut sink, ping_due);
    let prefix_len = sink.frames.len();
    supervisor
        .queue_disconnected(
            binding.connection_id,
            binding.tag.connection,
            stamp(deadline - 1),
        )
        .expect("disconnect before timeout observation");
    supervisor
        .queue_tick(stamp(deadline))
        .expect("queued timeout behind Down");
    let down = supervisor
        .drain_one(&mut sink)
        .expect("Down")
        .expect("Down outcome");
    assert_eq!(
        down.commands,
        vec![TransportCommand::Close {
            connection: binding.connection_id,
            epoch: binding.tag.connection
        }]
    );
    let timer = supervisor
        .drain_one(&mut sink)
        .expect("canceled timeout")
        .expect("Timer outcome");
    q2_assert_timer_only(&timer);
    assert_eq!(
        supervisor.snapshot(binding.id).expect("pending").tag,
        binding.tag
    );
    let completed = supervisor
        .drain_one(&mut sink)
        .expect("completion")
        .expect("completion outcome");
    assert!(matches!(
        completed.commands.as_slice(),
        [TransportCommand::ReconnectAfter { .. }]
    ));
    assert_eq!(
        supervisor
            .snapshot(binding.id)
            .expect("advanced")
            .tag
            .connection
            .get(),
        2
    );
    assert_eq!(
        sink.frames[prefix_len..]
            .iter()
            .filter(|frame| matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Down,
                        ..
                    },
                    ..
                })
            ))
            .count(),
        1
    );
    assert!(supervisor.drain_one(&mut sink).expect("empty").is_none());
}

impl RecordSink for Q2TimerFaultSink {
    fn fault_plan(&self) -> FaultPlan {
        FaultPlan::Timer(self.fault)
    }
    fn observe(&mut self, attempts: Vec<AttemptTrace>) {
        for attempt in attempts {
            if attempt.confirmed {
                self.confirmed.push(attempt.frame.clone());
            }
            self.attempts.push(attempt.frame);
        }
    }
}

#[test]
fn active_and_canceled_timers_preserve_all_storage_failure_dispositions() {
    for timeout in [false, true] {
        for canceled in [false, true] {
            for fault in [
                EpochPersistenceFault::Persist,
                EpochPersistenceFault::ReceiptMismatch,
                EpochPersistenceFault::InsufficientGate,
            ] {
                let binding = stream_binding(1, 1, 1, "BTCUSDT");
                let (mut supervisor, mut memory, ping_due) = q2_connected_fixture(&binding);
                let due = if timeout {
                    q2_send_initial_ping(&mut supervisor, &mut memory, ping_due)
                } else {
                    ping_due
                };
                if canceled {
                    supervisor
                        .queue_text(
                            binding.connection_id,
                            binding.tag.connection,
                            stamp(due - 1),
                            b"pong".to_vec(),
                        )
                        .expect("Pong before timer");
                }
                supervisor
                    .queue_tick(stamp(due))
                    .expect("timer observation");
                if canceled {
                    supervisor
                        .drain_one(&mut memory)
                        .expect("accept Pong")
                        .expect("Pong outcome");
                }
                let before = supervisor
                    .snapshot(binding.id)
                    .expect("state before failure");
                let durable_prefix = memory.frames;
                let mut sink = Q2TimerFaultSink {
                    fault,
                    attempts: Vec::new(),
                    confirmed: durable_prefix.clone(),
                };
                let error = supervisor
                    .drain_one(&mut sink)
                    .expect_err("timer error is explicit");
                let attempted = sink
                    .attempts
                    .first()
                    .expect("one unconfirmed Timer attempt");
                let expected = match fault {
                    EpochPersistenceFault::Persist => SupervisorError::Persistence(
                        PersistError::new("injected timer persistence error"),
                    ),
                    EpochPersistenceFault::ReceiptMismatch => {
                        SupervisorError::PersistenceReceiptMismatch {
                            expected: attempted.record_no,
                            actual: attempted.record_no.checked_next().expect("actual"),
                        }
                    }
                    EpochPersistenceFault::InsufficientGate => {
                        SupervisorError::PersistenceGateTooWeak {
                            required: RecordingGate::Durable,
                            achieved: RecordingGate::Written,
                        }
                    }
                };
                assert_eq!(error, expected);
                let before = failed_archive_snapshot(before, &supervisor);
                assert!(supervisor.is_halted());
                assert_eq!(
                    supervisor
                        .snapshot(binding.id)
                        .expect("no operation committed"),
                    before
                );
                assert_eq!(sink.confirmed, durable_prefix);
                assert_eq!(sink.attempts.len(), 1);
                for _ in 0..3 {
                    assert_eq!(
                        supervisor.drain_one(&mut sink),
                        Err(SupervisorError::Halted)
                    );
                    assert_eq!(
                        supervisor.queue_tick(stamp(due + 1)),
                        Err(SupervisorError::Halted)
                    );
                    assert_eq!(
                        supervisor.queue_text(
                            binding.connection_id,
                            binding.tag.connection,
                            stamp(due + 1),
                            b"pong".to_vec()
                        ),
                        Err(SupervisorError::Halted)
                    );
                }
                assert_eq!(sink.attempts.len(), 1);
                assert_eq!(sink.confirmed, durable_prefix);
            }
        }
    }
}

// The previous Q1 forensic fixtures remain recognizable below. The acceptance
// assertions now require the approved terminal-capture contract: the first
// missing candidate is consumed once, admitted prefix is preserved, and no
// later ingress can reuse that candidate. Historical forensic evidence remains
// in the REC-001D handoff; these tests do not transfer independent QA approval.

fn q1_blocker_capture(
    supervisor: &mut PublicWsSupervisor,
    sink: &mut MemorySink,
    binding: &StreamBinding,
    symbol: &str,
    at: u64,
) {
    connect_one(supervisor, sink, binding, at);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(at + 1),
            ack(symbol),
        )
        .expect("ack ingress");
    drain_all(supervisor, sink);
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(at + 2),
            snapshot(symbol, "7"),
        )
        .expect("snapshot ingress");
    drain_all(supervisor, sink);
    let state = supervisor.snapshot(binding.id).expect("capturing stream");
    assert_eq!(state.transport, Transport::Up);
    assert_eq!(state.subscription, SubscriptionState::CapturingRaw);
}

fn q1_blocker_next_generation(
    supervisor: &mut PublicWsSupervisor,
    sink: &mut MemorySink,
    binding: &StreamBinding,
    at: u64,
) -> StreamBinding {
    connect_one(supervisor, sink, binding, at);
    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(at + 1))
        .expect("disconnect ingress");
    let results = drain_all(supervisor, sink);
    assert_eq!(
        results
            .iter()
            .flat_map(|result| &result.commands)
            .filter(|command| matches!(command, TransportCommand::Close { .. }))
            .count(),
        1
    );
    let mut next = binding.clone();
    next.tag = supervisor
        .snapshot(binding.id)
        .expect("advanced stream")
        .tag;
    assert_eq!(next.tag.connection.get(), 2);
    next
}

fn q1_blocker_retained_raw_bytes(frames: &[RecordFrame]) -> usize {
    frames
        .iter()
        .map(|frame| match &frame.value {
            Record::RawInput(raw) => raw.bytes.len(),
            _ => 0,
        })
        .sum()
}

fn q1_blocker_target(gap: &Gap) -> &GapTarget {
    let GapScope::ExplicitTargets(targets) = &gap.scope else {
        panic!("explicit diagnostic/loss scope required");
    };
    assert_eq!(targets.len(), 1);
    &targets[0]
}

#[test]
fn q1_single_stream_legal_five_consumes_failure_once_and_drains_exact_prefix() {
    let old = stream_binding(1, 1, 1, "BTCUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 65536,
        max_raw_message_bytes: 65536,
        max_total_items: 5,
    };
    let mut supervisor = canonical_supervisor(vec![old.clone()], policy, RecordingGate::Durable);
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    let current = q1_blocker_next_generation(&mut supervisor, &mut sink, &old, 1);
    let at = RECONNECT_MAX_NS_V1 + 100;
    q1_blocker_capture(&mut supervisor, &mut sink, &current, "BTCUSDT", at);
    assert_eq!(
        supervisor
            .snapshot(old.id)
            .expect("state")
            .capture_attempt_frontier,
        2
    );
    let persisted_before = sink.frames.len();
    for index in 0..3 {
        supervisor
            .queue_text(
                old.connection_id,
                old.tag.connection,
                stamp(at + 10 + index),
                vec![b'x'; 1_100_000],
            )
            .expect("counted stale diagnostic");
        let state = supervisor.snapshot(old.id).expect("bounded accounting");
        assert_eq!(state.capture_attempt_frontier, 3 + index);
        assert_eq!(state.queued_raw_frames, 0);
        assert_eq!(state.queued_raw_bytes, 0);
        assert_eq!(supervisor.queued_items(), index as usize + 1);
    }
    assert_eq!(
        supervisor.queue_text(
            current.connection_id,
            current.tag.connection,
            stamp(at + 20),
            b"{".to_vec()
        ),
        Err(SupervisorError::QueueExhausted { stream: old.id })
    );
    let failure = supervisor
        .inner
        .terminal_failure(old.id)
        .expect("exact failure identity");
    assert_eq!(failure.stream, old.id);
    assert_eq!(failure.connection, old.connection_id);
    assert_eq!(failure.observed_tag, current.tag);
    assert_eq!(failure.current_epoch, current.tag.connection);
    assert_eq!(failure.context, active_context());
    assert_eq!(failure.stamp.unix_ns, stamp(at + 20).unix_ns);
    assert_eq!(failure.stamp.monotonic_ns, at + 20);
    assert_eq!(
        failure.attempt,
        session::AttemptIdentity::Candidate(id(CaptureAttemptNo::new(6)))
    );
    assert_eq!(failure.input_class, session::InputClass::Raw);
    assert_eq!(failure.cause, session::FailureCause::QueueOverflow);
    let retention = supervisor.inner.retention_report();
    assert_eq!(retention.ownership.work_used, 3);
    assert_eq!(retention.ownership.reserved_scopes, 1);
    assert_eq!(retention.ownership.item_cap, 5);
    assert_eq!(retention.filled_terminal_slots, 1);
    let terminal = supervisor.snapshot(old.id).expect("terminal capture");
    assert_eq!(terminal.capture_attempt_frontier, 6);
    assert_eq!(supervisor.queued_items(), 3);
    assert!(
        !supervisor.is_halted(),
        "scope terminal is not a storage stop"
    );
    for index in 0..32 {
        supervisor
            .queue_text(
                current.connection_id,
                current.tag.connection,
                stamp(at + 30 + index),
                update("BTCUSDT", 10, 11),
            )
            .expect("AlreadyTerminated is explicit, repeat retains nothing");
        assert_eq!(
            supervisor.snapshot(old.id).expect("immutable failure"),
            terminal
        );
        assert_eq!(supervisor.queued_items(), 3);
    }
    let before_repeats = supervisor.inner.retention_report().ownership;
    let newer_epoch = id(ConnectionEpoch::new(3));
    for epoch in [old.tag.connection, current.tag.connection, newer_epoch] {
        for report in [
            supervisor.inner.queue_connected(
                &mut supervisor.turn,
                old.connection_id,
                epoch,
                stamp(at + 100),
            ),
            supervisor.inner.queue_disconnected(
                &mut supervisor.turn,
                old.connection_id,
                epoch,
                stamp(at + 101),
            ),
            supervisor.inner.queue_text(
                &mut supervisor.turn,
                old.connection_id,
                epoch,
                stamp(at + 102),
                b"pong",
            ),
        ] {
            assert_eq!(report.outcome, Ok(AdmissionOutcome::AlreadyTerminated));
            assert_eq!(report.failure, Some(failure));
            assert!(report.commands.is_empty());
            assert!(matches!(
                report.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
        }
    }
    let tick = supervisor
        .inner
        .queue_tick(&mut supervisor.turn, stamp(at + HEARTBEAT_INTERVAL_NS));
    assert_eq!(tick.outcome, Ok(AdmissionOutcome::Admitted));
    assert!(tick.admitted_scopes.iter().all(Option::is_none));
    assert!(tick.commands.is_empty());
    let restart = supervisor.inner.start_commands(&mut supervisor.turn);
    assert_eq!(restart.outcome, Err(SupervisorError::AlreadyStarted));
    assert!(restart.commands.is_empty());
    assert_eq!(
        supervisor.inner.retention_report().ownership,
        before_repeats
    );
    assert_eq!(supervisor.inner.terminal_failure(old.id), Some(failure));
    assert_eq!(
        supervisor
            .snapshot(old.id)
            .expect("accounted pre-cut prefix")
            .accounted_attempt_frontier,
        2
    );
    let results = drain_all(&mut supervisor, &mut sink);
    assert!(results.iter().all(|result| result.commands.is_empty()));
    let diagnostics: Vec<_> = sink.frames[persisted_before..]
        .iter()
        .filter(|frame| {
            !matches!(
                frame.value,
                Record::Control(ControlRecord {
                    value: Control::Recording(_),
                    ..
                })
            )
        })
        .collect();
    assert_eq!(diagnostics.len(), 6);
    for (index, pair) in diagnostics.as_chunks::<2>().0.iter().enumerate() {
        let Record::RawInput(raw) = &pair[0].value else {
            panic!("stale raw first")
        };
        assert_eq!(raw.stream, old.id);
        assert_eq!(raw.tag, old.tag);
        assert_eq!(raw.attempt.get(), 3 + index as u64);
        assert_eq!(raw.context.monotonic_ns.get(), at + 10 + index as u64);
        assert!(raw.bytes.is_empty());
        let Record::Gap(gap) = &pair[1].value else {
            panic!("stale diagnostic follows")
        };
        assert_eq!(gap.reason, Reason::Unknown);
        assert_eq!(q1_blocker_target(gap).range, None);
        assert_eq!(q1_blocker_target(gap).loss_count, None);
    }
    assert!(
        sink.frames[persisted_before..]
            .iter()
            .all(|frame| !matches!(&frame.value,
        Record::RawInput(raw) if raw.attempt.get() == 6))
    );
    assert_eq!(
        supervisor
            .snapshot(old.id)
            .expect("no reuse")
            .capture_attempt_frontier,
        6
    );
    assert_eq!(
        supervisor
            .snapshot(old.id)
            .expect("admitted diagnostics accounted")
            .accounted_attempt_frontier,
        5
    );
    assert_eq!(supervisor.queued_items(), 0);
    assert_eq!(supervisor.drain_one(&mut sink), Ok(None));
}

#[test]
fn q1_two_stream_legal_nine_reserves_both_failures_and_services_neighbor_when_work_frees() {
    for release_before_neighbor in [false, true] {
        let old_a = stream_binding(1, 1, 1, "BTCUSDT");
        let b = stream_binding(2, 2, 2, "ETHUSDT");
        let policy = QueuePolicy {
            max_raw_frames_per_stream: 1,
            max_raw_bytes_per_stream: 65536,
            max_raw_message_bytes: 65536,
            max_total_items: 9,
        };
        let mut supervisor = canonical_supervisor(
            vec![old_a.clone(), b.clone()],
            policy,
            RecordingGate::Durable,
        );
        let mut sink = MemorySink::default();
        supervisor.start_commands().expect("start");
        let a = q1_blocker_next_generation(&mut supervisor, &mut sink, &old_a, 1);
        let at = RECONNECT_MAX_NS_V1 + 100;
        q1_blocker_capture(&mut supervisor, &mut sink, &a, "BTCUSDT", at);
        q1_blocker_capture(&mut supervisor, &mut sink, &b, "ETHUSDT", at + 3);
        let b_before = supervisor.snapshot(b.id).expect("neighbor ready");
        let persisted_before = sink.frames.len();
        for index in 0..6 {
            supervisor
                .queue_text(
                    old_a.connection_id,
                    old_a.tag.connection,
                    stamp(at + 10 + index),
                    vec![b'x'; 1_100_000],
                )
                .expect("counted stale a");
            assert_eq!(supervisor.queued_items(), index as usize + 1);
        }
        assert_eq!(
            supervisor.queue_text(
                a.connection_id,
                a.tag.connection,
                stamp(at + 20),
                b"{".to_vec()
            ),
            Err(SupervisorError::QueueExhausted { stream: a.id })
        );
        assert_eq!(
            supervisor
                .snapshot(a.id)
                .expect("first failure")
                .capture_attempt_frontier,
            9
        );
        if release_before_neighbor {
            supervisor
                .drain_one(&mut sink)
                .expect("drain one")
                .expect("first admitted owner");
            supervisor
                .queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 21),
                    update("ETHUSDT", 10, 11),
                )
                .expect("neighbor remains serviceable");
        } else {
            assert_eq!(
                supervisor.queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 21),
                    b"{".to_vec()
                ),
                Err(SupervisorError::QueueExhausted { stream: b.id })
            );
        }
        let terminal_a = supervisor.snapshot(a.id).expect("a immutable");
        supervisor
            .queue_text(
                a.connection_id,
                a.tag.connection,
                stamp(at + 22),
                update("BTCUSDT", 10, 11),
            )
            .expect("AlreadyTerminated");
        assert_eq!(supervisor.snapshot(a.id).expect("no attempt10"), terminal_a);
        drain_all(&mut supervisor, &mut sink);
        let diagnostics = &sink.frames[persisted_before..];
        let stale: Vec<_> = diagnostics
            .iter()
            .filter_map(|frame| match &frame.value {
                Record::RawInput(raw) if raw.stream == a.id => Some(raw),
                _ => None,
            })
            .collect();
        assert_eq!(stale.len(), 6);
        for (index, raw) in stale.iter().enumerate() {
            assert!(raw.bytes.is_empty());
            assert_eq!(raw.attempt.get(), 3 + index as u64);
            assert_eq!(raw.tag, old_a.tag);
            assert_eq!(raw.context.monotonic_ns.get(), at + 10 + index as u64);
        }
        let neighbor = supervisor.snapshot(b.id).expect("neighbor frontier");
        assert_eq!(
            neighbor.capture_attempt_frontier,
            b_before.capture_attempt_frontier + 1
        );
        assert_eq!(
            diagnostics
                .iter()
                .filter(|frame| matches!(&frame.value,
            Record::RawInput(raw) if raw.stream == b.id))
                .count(),
            usize::from(release_before_neighbor)
        );
        assert_eq!(
            diagnostics
                .iter()
                .filter(|frame| matches!(
                    &frame.value,
                    Record::Control(ControlRecord {
                        value: Control::Recording(RecordingEvidence {
                            health: RecordingHealth::Failed,
                            reason: Reason::QueueOverflow,
                            ..
                        }),
                        ..
                    })
                ))
                .count(),
            1
        );
        assert_eq!(supervisor.queued_items(), 0);
        assert!(!supervisor.is_halted());
    }
}

#[test]
fn q1_noncoalescing_saturation_preserves_admitted_raw_control_and_exact_scopes() {
    let old_a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 65536,
        max_raw_message_bytes: 65536,
        max_total_items: 9,
    };
    let mut supervisor = canonical_supervisor(
        vec![old_a.clone(), b.clone()],
        policy,
        RecordingGate::Durable,
    );
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    let a = q1_blocker_next_generation(&mut supervisor, &mut sink, &old_a, 1);
    let at = RECONNECT_MAX_NS_V1 + 100;
    q1_blocker_capture(&mut supervisor, &mut sink, &a, "BTCUSDT", at);
    q1_blocker_capture(&mut supervisor, &mut sink, &b, "ETHUSDT", at + 3);
    let persisted_before = sink.frames.len();
    let raw = update("BTCUSDT", 10, 11);
    supervisor
        .queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(at + 10),
            raw.clone(),
        )
        .expect("raw");
    supervisor
        .queue_connected(b.connection_id, b.tag.connection, stamp(at + 11))
        .expect("control");
    supervisor
        .queue_text(
            a.connection_id,
            old_a.tag.connection,
            stamp(at + 12),
            vec![b'x'; 1_100_000],
        )
        .expect("stale a4");
    supervisor
        .queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(at + 13),
            b"{".to_vec(),
        )
        .expect("gap a5");
    supervisor
        .queue_text(
            b.connection_id,
            b.tag.connection,
            stamp(at + 14),
            b"{".to_vec(),
        )
        .expect("gap b3");
    supervisor
        .queue_text(
            a.connection_id,
            old_a.tag.connection,
            stamp(at + 15),
            vec![b'x'; 1_100_000],
        )
        .expect("stale a6");
    assert_eq!(supervisor.queued_items(), 6);
    assert_eq!(
        supervisor.queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(at + 16),
            b"{".to_vec()
        ),
        Err(SupervisorError::QueueExhausted { stream: a.id })
    );
    let terminal = supervisor.snapshot(a.id).expect("terminal a7");
    assert_eq!(terminal.capture_attempt_frontier, 7);
    assert_eq!(terminal.queued_raw_frames, 1);
    assert_eq!(terminal.queued_raw_bytes, raw.len());
    supervisor
        .queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(at + 17),
            b"{".to_vec(),
        )
        .expect("AlreadyTerminated");
    assert_eq!(
        supervisor.snapshot(a.id).expect("immutable terminal"),
        terminal
    );
    let results = drain_all(&mut supervisor, &mut sink);
    let returned: Vec<_> = results.iter().flat_map(|result| &result.commands).collect();
    assert_eq!(returned.len(), 1);
    assert!(
        matches!(returned[0], TransportCommand::SendText { connection, epoch, text }
        if *connection == b.connection_id && *epoch == b.tag.connection && text.contains("subscribe"))
    );
    let frames = &sink.frames[persisted_before..];
    assert_eq!(q1_blocker_retained_raw_bytes(frames), raw.len());
    let Record::RawInput(admitted) = &frames[0].value else {
        panic!("raw stays first")
    };
    assert_eq!(admitted.bytes, raw);
    assert_eq!(admitted.attempt.get(), 3);
    assert!(matches!(&frames[1].value, Record::Control(ControlRecord {
        value: Control::Transport { connection, epoch, value: Transport::Up }, .. })
        if *connection == b.connection_id && *epoch == b.tag.connection));
    let ranges: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::Gap(gap) if gap.reason == Reason::QueueOverflow => {
                let target = q1_blocker_target(gap);
                let (first, last) = target.range.expect("exact range");
                assert_eq!(target.loss_count, Some(1));
                Some((target.stream, target.tag, first.get(), last.get()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(ranges, vec![(a.id, a.tag, 5, 5), (b.id, b.tag, 3, 3)]);
    let stale: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::RawInput(raw) if raw.bytes.is_empty() => Some((
                raw.stream,
                raw.tag,
                raw.attempt.get(),
                raw.context.monotonic_ns.get(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        stale,
        vec![(a.id, old_a.tag, 4, at + 12), (a.id, old_a.tag, 6, at + 15)]
    );
    assert!(frames.iter().all(|frame| !matches!(&frame.value,
        Record::RawInput(raw) if raw.stream == a.id && raw.attempt.get() >= 7)));
    assert_eq!(supervisor.queued_items(), 0);
    assert!(!supervisor.is_halted());
}

#[test]
fn r1_pre_cut_tail_gap_is_frozen_for_contiguous_neighbor_loss_with_free_or_exhausted_work() {
    for free_work in [false, true] {
        let old_a = stream_binding(1, 1, 1, "BTCUSDT");
        let b = stream_binding(2, 2, 2, "ETHUSDT");
        let policy = QueuePolicy {
            max_raw_frames_per_stream: 1,
            max_raw_bytes_per_stream: 65536,
            max_raw_message_bytes: 65536,
            max_total_items: 9,
        };
        let mut supervisor = canonical_supervisor(
            vec![old_a.clone(), b.clone()],
            policy,
            RecordingGate::Durable,
        );
        let mut sink = MemorySink::default();
        supervisor.start_commands().expect("start");
        let a = q1_blocker_next_generation(&mut supervisor, &mut sink, &old_a, 1);
        let at = RECONNECT_MAX_NS_V1 + 100;
        connect_one(&mut supervisor, &mut sink, &b, at);
        supervisor
            .queue_text(
                b.connection_id,
                b.tag.connection,
                stamp(at + 1),
                ack("ETHUSDT"),
            )
            .expect("B Raw1");
        drain_all(&mut supervisor, &mut sink);
        let before = sink.frames.len();
        for index in 0..5 {
            supervisor
                .queue_text(
                    a.connection_id,
                    old_a.tag.connection,
                    stamp(at + 10 + index),
                    vec![b'x'; 1_100_000],
                )
                .expect("five separately stamped pre-cut owners");
        }
        for index in 0..2 {
            supervisor
                .queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 30 + index),
                    vec![b'x'; 65537],
                )
                .expect("B contiguous pre-cut loss");
        }
        assert_eq!(supervisor.queued_items(), 6);
        assert_eq!(
            supervisor.queue_text(
                a.connection_id,
                a.tag.connection,
                stamp(at + 35),
                b"{".to_vec()
            ),
            Err(SupervisorError::QueueExhausted { stream: a.id })
        );
        if free_work {
            supervisor
                .drain_one(&mut sink)
                .expect("release earlier pre-cut W")
                .expect("one owner");
            supervisor
                .queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 40),
                    vec![b'x'; 65537],
                )
                .expect("new separately counted post-cut loss");
            assert_eq!(supervisor.queued_items(), 6);
        } else {
            assert_eq!(
                supervisor.queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 40),
                    vec![b'x'; 65537]
                ),
                Err(SupervisorError::QueueExhausted { stream: b.id })
            );
            assert_eq!(supervisor.queued_items(), 6);
        }
        let retained = supervisor.inner.retention_report();
        assert_eq!(retained.ownership.work_used, 6);
        assert_eq!(retained.ownership.reserved_scopes, 2);
        assert_eq!(retained.ownership.reserved_archive, 1);
        assert_eq!(retained.ownership.item_cap, 9);
        assert_eq!(retained.ownership.pre_cut, 6 - usize::from(free_work));
        assert_eq!(retained.ownership.post_cut, usize::from(free_work));
        if !free_work {
            let failure = supervisor
                .inner
                .terminal_failure(b.id)
                .expect("exact neighbor failure");
            assert_eq!(
                failure.attempt,
                session::AttemptIdentity::Candidate(id(CaptureAttemptNo::new(4)))
            );
            assert_eq!(failure.observed_tag, b.tag);
            assert_eq!(failure.stamp.monotonic_ns, at + 40);
            assert_eq!(failure.cause, session::FailureCause::QueueOverflow);
        }
        drain_all(&mut supervisor, &mut sink);
        let frames = &sink.frames[before..];
        let marker = frames
            .iter()
            .position(|frame| {
                matches!(
                    &frame.value,
                    Record::Control(ControlRecord {
                        value: Control::Recording(RecordingEvidence {
                            health: RecordingHealth::Failed,
                            reason: Reason::QueueOverflow,
                            ..
                        }),
                        ..
                    })
                )
            })
            .expect("one archive-wide marker");
        let gaps: Vec<_> = frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| match &frame.value {
                Record::Gap(gap)
                    if gap.reason == Reason::QueueOverflow
                        && q1_blocker_target(gap).stream == b.id =>
                {
                    Some((index, gap))
                }
                _ => None,
            })
            .collect();
        assert_eq!(gaps.len(), 1 + usize::from(free_work));
        let (pre_index, pre) = gaps[0];
        let pre_target = q1_blocker_target(pre);
        assert_eq!(
            pre_target
                .range
                .map(|(first, last)| (first.get(), last.get())),
            Some((2, 3))
        );
        assert_eq!(pre_target.loss_count, Some(2));
        assert_eq!(pre.context.monotonic_ns.get(), at + 30);
        assert!(pre_index < marker, "pre-cut owner drains before marker");
        if free_work {
            let (post_index, post) = gaps[1];
            let target = q1_blocker_target(post);
            assert_eq!(
                target.range.map(|(first, last)| (first.get(), last.get())),
                Some((4, 4))
            );
            assert_eq!(target.loss_count, Some(1));
            assert_eq!(post.context.monotonic_ns.get(), at + 40);
            assert!(
                marker < post_index,
                "post-cut owner cannot move before marker"
            );
        }
        assert_eq!(
            supervisor
                .snapshot(b.id)
                .expect("B consumed frontier")
                .capture_attempt_frontier,
            4
        );
        assert_eq!(
            supervisor
                .snapshot(b.id)
                .expect("truthful accounted frontier")
                .accounted_attempt_frontier,
            if free_work { 4 } else { 3 }
        );
        assert_eq!(supervisor.queued_items(), 0);
        assert!(!supervisor.is_halted());
    }
}

#[test]
fn canonical_terminal_close_reclaim_survives_all_marker_faults_and_diagnostic_closure() {
    for fault in [
        EpochPersistenceFault::Persist,
        EpochPersistenceFault::ReceiptMismatch,
        EpochPersistenceFault::InsufficientGate,
    ] {
        let binding = stream_binding(1, 1, 1, "BTCUSDT");
        let policy = QueuePolicy {
            max_raw_frames_per_stream: 1,
            max_raw_bytes_per_stream: 4096,
            max_raw_message_bytes: 4096,
            max_total_items: 5,
        };
        let mut supervisor =
            canonical_supervisor(vec![binding.clone()], policy, RecordingGate::Durable);
        let mut trace = MemorySink::default();
        supervisor.start_commands().expect("start");
        connect_one(&mut supervisor, &mut trace, &binding, 1);
        for at in 2..=4 {
            let report = supervisor.inner.queue_connected(
                &mut supervisor.turn,
                binding.connection_id,
                binding.tag.connection,
                stamp(at),
            );
            assert_eq!(report.outcome, Ok(AdmissionOutcome::Admitted));
            assert!(report.commands.is_empty());
        }
        let failure = supervisor.inner.queue_text(
            &mut supervisor.turn,
            binding.connection_id,
            binding.tag.connection,
            stamp(5),
            b"{",
        );
        assert_eq!(
            failure.outcome,
            Err(SupervisorError::QueueExhausted { stream: binding.id })
        );
        assert!(matches!(
            failure.session_disposition,
            session::SessionDisposition::DiagnosticOnly { .. }
        ));
        assert_eq!(failure.commands.len(), 1);
        let close_owner = failure
            .close_owner
            .expect("mandatory Close owner is explicit even on error");
        let command = failure
            .commands
            .into_iter()
            .next()
            .expect("first affine Close");
        let ledger = supervisor.inner.retention_report().ownership;
        assert_eq!(ledger.work_used, 3);
        assert_eq!(
            ledger.reserved_scopes + ledger.reserved_archive + ledger.work_used,
            5
        );
        drop(command);
        let owner = supervisor.owner.as_mut().expect("canonical owner");
        assert!(matches!(
            owner.outstanding_close_owners().iter().next(),
            Some(session::CloseOwnerView {
                state: session::CloseState::Pending,
                ..
            })
        ));
        let session::CloseLeaseReport::Leased(lease) =
            owner.reclaim_close(&mut supervisor.turn, close_owner.clone())
        else {
            panic!("Drop must leave the same owner reclaimable")
        };
        assert!(matches!(
            owner.reclaim_close(&mut supervisor.turn, close_owner.clone()),
            session::CloseLeaseReport::AlreadyLeased
        ));
        let mut physical_attempts = 0;
        let dispatched = owner.dispatch(&mut supervisor.turn, lease.into_command(), |view| {
            assert_eq!(view.epoch, binding.tag.connection);
            assert_eq!(view.kind, &session::CommandKind::Close);
            physical_attempts += 1;
            Err::<(), _>("ambiguous physical close")
        });
        assert!(matches!(
            dispatched,
            session::DispatchReport::DispatchFailed {
                effect: session::AmbiguousEffect::Unknown,
                ..
            }
        ));
        assert_eq!(physical_attempts, 1);
        let session::CloseLeaseReport::Leased(lease) =
            owner.reclaim_close(&mut supervisor.turn, close_owner.clone())
        else {
            panic!("dispatch error keeps the same Close Pending")
        };
        let mut foreign =
            canonical_supervisor(vec![binding.clone()], policy, RecordingGate::Durable);
        assert!(matches!(
            owner.reclaim_close(&mut foreign.turn, close_owner.clone()),
            session::CloseLeaseReport::Rejected(session::AuthorityError::AuthorityMismatch)
        ));
        let denied = owner.dispatch(&mut foreign.turn, lease.into_command(), |_| {
            panic!("foreign authority must not execute an effect") as Result<(), ()>
        });
        let session::DispatchReport::Denied {
            reason: session::AuthorityError::AuthorityMismatch,
            command,
        } = denied
        else {
            panic!("foreign dispatch returns the same valid lease")
        };
        assert!(matches!(
            owner.reclaim_close(&mut supervisor.turn, close_owner.clone()),
            session::CloseLeaseReport::AlreadyLeased
        ));
        drop(command);
        assert_eq!(
            supervisor.inner.retention_report().ownership,
            ledger,
            "Drop/reclaim/error/foreign denial retain the same counted owner"
        );
        while supervisor.inner.queued_items() != 0 {
            let report = supervisor
                .inner
                .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
            assert!(matches!(
                report.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
            let result = report
                .outcome
                .expect("admitted prefix drain")
                .expect("admitted record owner");
            assert!(
                result.commands.is_empty(),
                "historical Connected cannot reopen terminated capture"
            );
            drop(result);
        }
        let owner = supervisor.owner.as_mut().expect("owner");
        let marker_record = owner
            .watermarks()
            .written
            .expect("admitted prefix")
            .checked_next()
            .expect("marker no");
        let kind = match fault {
            EpochPersistenceFault::Persist => recording::SinkFaultKind::BeforeWrite(
                session::PersistError::new("marker storage error"),
            ),
            EpochPersistenceFault::ReceiptMismatch => recording::SinkFaultKind::ReceiptMismatch {
                through: marker_record.checked_next().expect("mismatched receipt"),
            },
            EpochPersistenceFault::InsufficientGate => recording::SinkFaultKind::WeakGate {
                achieved: RecordingGate::Written,
            },
        };
        owner
            .set_sink_fault(
                &mut supervisor.turn,
                Some(recording::SinkFault {
                    at: marker_record,
                    kind,
                }),
            )
            .expect("bounded negative fault on same writer");
        let marker = supervisor
            .inner
            .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
        assert!(marker.outcome.is_err());
        assert!(matches!(
            marker.session_disposition,
            session::SessionDisposition::DiagnosticOnly { .. }
        ));
        assert!(matches!(
            owner.session_status().marker,
            session::MarkerState::Unconfirmed(_)
        ));
        assert!(owner.session_status().failed);
        assert!(owner.session_status().storage_stopped.is_some());
        let closure = owner.close_diagnostic(&mut supervisor.turn);
        assert!(matches!(
            closure.outcome,
            Ok(recording::DiagnosticCloseState::Closed)
        ));
        assert!(closure.physical_report.descriptor_closed);
        assert_eq!(closure.input_completeness, InputQuality::Unknown);
        let before_reclaim = supervisor.inner.retention_report().ownership;
        let session::CloseLeaseReport::Leased(lease) =
            owner.reclaim_close(&mut supervisor.turn, close_owner.clone())
        else {
            panic!("Close is reclaimable after StorageStopped and descriptor closure")
        };
        assert_eq!(
            supervisor.inner.retention_report().ownership,
            before_reclaim
        );
        assert!(matches!(
            owner.dispatch(&mut supervisor.turn, lease.into_command(), |view| {
                assert_eq!(view.epoch, binding.tag.connection);
                assert_eq!(view.kind, &session::CommandKind::Close);
                physical_attempts += 1;
                Ok::<(), ()>(())
            }),
            session::DispatchReport::Dispatched
        ));
        assert_eq!(
            physical_attempts, 2,
            "retry is same epoch idempotent Close, not exactly-once physical execution"
        );
        assert!(owner.outstanding_close_owners().iter().next().is_none());
        assert!(matches!(
            owner.reclaim_close(&mut supervisor.turn, close_owner),
            session::CloseLeaseReport::AlreadySettled
        ));
        assert_eq!(
            supervisor.inner.retention_report().ownership,
            before_reclaim
        );
        let mut reader = WalReader::open(&supervisor.temp.path).expect("diagnostic reader");
        let mut marker_count = 0;
        while let Some(frame) = reader.next_record().expect("prefix") {
            assert!(!matches!(
                frame.value,
                Record::SegmentSeal(_) | Record::ArchiveSeal(_)
            ));
            if matches!(
                frame.value,
                Record::Control(ControlRecord {
                    value: Control::Recording(_),
                    ..
                })
            ) {
                marker_count += 1;
            }
        }
        assert_eq!(
            marker_count,
            usize::from(!matches!(fault, EpochPersistenceFault::Persist))
        );
        assert_eq!(
            reader.report().status,
            recording::ArchiveStatus::ValidPrefixIncomplete
        );
        assert_eq!(reader.report().input_quality, None);
    }
}

#[test]
fn canonical_borrowed_quiescence_retries_foreign_ticket_and_single_proof_issuance() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = canonical_supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Flushed,
    );
    supervisor.start_commands().expect("start");
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(1))
        .expect("admitted before close");
    let ticket = supervisor
        .owner
        .as_mut()
        .expect("owner")
        .begin_finalization(&mut supervisor.turn)
        .expect("close ticket");
    let ledger = supervisor.inner.retention_report().ownership;
    for _ in 0..3 {
        let report = supervisor.inner.quiesce(&mut supervisor.turn, &ticket);
        let session::QuiescenceReport::NotReady(unsettled) = report else {
            panic!("same ticket remains retryable")
        };
        assert_eq!(unsettled.queued, 1);
        assert_eq!(unsettled.work_total, 1);
        assert_eq!(supervisor.inner.retention_report().ownership, ledger);
    }
    let mut foreign = canonical_supervisor(
        vec![binding],
        QueuePolicy::default(),
        RecordingGate::Flushed,
    );
    let foreign_ticket = foreign
        .owner
        .as_mut()
        .expect("foreign owner")
        .begin_finalization(&mut foreign.turn)
        .expect("foreign ticket");
    assert!(matches!(
        supervisor
            .inner
            .quiesce(&mut supervisor.turn, &foreign_ticket),
        session::QuiescenceReport::Rejected(session::AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        supervisor.inner.quiesce(&mut foreign.turn, &ticket),
        session::QuiescenceReport::Rejected(session::AuthorityError::AuthorityMismatch)
    ));
    assert_eq!(supervisor.inner.retention_report().ownership, ledger);
    let drained = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("explicit caller drain while Closing")
        .expect("admitted record");
    assert!(
        drained.commands.is_empty(),
        "Closing cannot issue Subscribe"
    );
    assert!(
        matches!(
            supervisor.inner.quiesce(&mut supervisor.turn, &ticket),
            session::QuiescenceReport::NotReady(_)
        ),
        "held result is still counted"
    );
    drop(drained);
    let session::QuiescenceReport::Ready(proof) =
        supervisor.inner.quiesce(&mut supervisor.turn, &ticket)
    else {
        panic!("settlement makes the same ticket ready")
    };
    assert!(matches!(
        supervisor.inner.quiesce(&mut supervisor.turn, &ticket),
        session::QuiescenceReport::TicketConsumed
    ));
    supervisor
        .owner
        .as_mut()
        .expect("owner")
        .finalize(&mut supervisor.turn, proof)
        .expect("one-use proof finalizes once");
    assert!(matches!(
        supervisor.inner.quiesce(&mut supervisor.turn, &ticket),
        session::QuiescenceReport::TicketConsumed
    ));
}

#[test]
fn canonical_storage_failure_during_closing_invalidates_borrowed_ticket_terminally() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let mut supervisor = canonical_supervisor(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Durable,
    );
    supervisor.start_commands().expect("start");
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(1))
        .expect("admitted input");
    let owner = supervisor.owner.as_mut().expect("owner");
    let ticket = owner
        .begin_finalization(&mut supervisor.turn)
        .expect("ticket");
    let next = owner
        .watermarks()
        .written
        .expect("bootstrap")
        .checked_next()
        .expect("next");
    owner
        .set_sink_fault(
            &mut supervisor.turn,
            Some(recording::SinkFault {
                at: next,
                kind: recording::SinkFaultKind::BeforeWrite(session::PersistError::new(
                    "failure during Closing",
                )),
            }),
        )
        .expect("same owner negative fault");
    assert!(matches!(
        supervisor.inner.quiesce(&mut supervisor.turn, &ticket),
        session::QuiescenceReport::NotReady(_)
    ));
    let drain = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
    assert!(drain.outcome.is_err());
    assert!(matches!(
        drain.session_disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    assert_eq!(
        owner.session_status().lifecycle,
        session::SessionLifecycle::DiagnosticClosing
    );
    for _ in 0..3 {
        assert!(matches!(
            supervisor.inner.quiesce(&mut supervisor.turn, &ticket),
            session::QuiescenceReport::FinalizationInvalidated(
                session::AuthorityError::ArchiveFailed
            )
        ));
    }
    assert!(matches!(
        owner.begin_finalization(&mut supervisor.turn),
        Err(recording::OwnerError::Authority(
            session::AuthorityError::ArchiveFailed
        ))
    ));
    let mut reader = WalReader::open(&supervisor.temp.path).expect("unsealed prefix");
    while let Some(frame) = reader.next_record().expect("prefix") {
        assert!(!matches!(
            frame.value,
            Record::SegmentSeal(_) | Record::ArchiveSeal(_)
        ));
    }
    assert_eq!(
        reader.report().status,
        recording::ArchiveStatus::ValidPrefixIncomplete
    );
    assert_eq!(reader.report().input_quality, None);
}

#[test]
fn r1_marker_fault_preserves_frozen_tail_cut_and_pending_post_cut_owner() {
    for free_work in [false, true] {
        for fault in [
            EpochPersistenceFault::Persist,
            EpochPersistenceFault::ReceiptMismatch,
            EpochPersistenceFault::InsufficientGate,
        ] {
            let old_a = stream_binding(1, 1, 1, "BTCUSDT");
            let b = stream_binding(2, 2, 2, "ETHUSDT");
            let policy = QueuePolicy {
                max_raw_frames_per_stream: 1,
                max_raw_bytes_per_stream: 65536,
                max_raw_message_bytes: 65536,
                max_total_items: 9,
            };
            let mut supervisor = canonical_supervisor(
                vec![old_a.clone(), b.clone()],
                policy,
                RecordingGate::Durable,
            );
            let mut trace = MemorySink::default();
            supervisor.start_commands().expect("start");
            let a = q1_blocker_next_generation(&mut supervisor, &mut trace, &old_a, 1);
            let at = RECONNECT_MAX_NS_V1 + 100;
            connect_one(&mut supervisor, &mut trace, &b, at);
            supervisor
                .queue_text(
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 1),
                    ack("ETHUSDT"),
                )
                .expect("B Raw1");
            drain_all(&mut supervisor, &mut trace);
            for index in 0..5 {
                supervisor
                    .queue_text(
                        a.connection_id,
                        old_a.tag.connection,
                        stamp(at + 10 + index),
                        vec![b'x'; 1_100_000],
                    )
                    .expect("five PreCut owners");
            }
            for index in 0..2 {
                supervisor
                    .queue_text(
                        b.connection_id,
                        b.tag.connection,
                        stamp(at + 30 + index),
                        vec![b'x'; 65537],
                    )
                    .expect("PreCut B loss2..3");
            }
            assert_eq!(
                supervisor.queue_text(
                    a.connection_id,
                    a.tag.connection,
                    stamp(at + 35),
                    b"{".to_vec()
                ),
                Err(SupervisorError::QueueExhausted { stream: a.id })
            );
            if free_work {
                supervisor
                    .drain_one(&mut trace)
                    .expect("release one PreCut owner")
                    .expect("owner");
                supervisor
                    .queue_text(
                        b.connection_id,
                        b.tag.connection,
                        stamp(at + 40),
                        vec![b'x'; 65537],
                    )
                    .expect("separate PostCut B4");
            } else {
                assert_eq!(
                    supervisor.queue_text(
                        b.connection_id,
                        b.tag.connection,
                        stamp(at + 40),
                        vec![b'x'; 65537]
                    ),
                    Err(SupervisorError::QueueExhausted { stream: b.id })
                );
            }
            let retained = supervisor.inner.retention_report();
            assert_eq!(retained.ownership.work_used, 6);
            assert_eq!(retained.ownership.pre_cut, 6 - usize::from(free_work));
            assert_eq!(retained.ownership.post_cut, usize::from(free_work));
            assert_eq!(
                retained.ownership.work_used
                    + retained.ownership.reserved_scopes
                    + retained.ownership.reserved_archive,
                9
            );
            while supervisor.inner.retention_report().ownership.pre_cut != 0 {
                let report = supervisor
                    .inner
                    .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
                assert!(matches!(
                    report.session_disposition,
                    session::SessionDisposition::DiagnosticOnly { .. }
                ));
                drop(
                    report
                        .outcome
                        .expect("original PreCut prefix drains")
                        .expect("record owner"),
                );
            }
            assert_eq!(
                supervisor
                    .snapshot(b.id)
                    .expect("PreGap accounted")
                    .accounted_attempt_frontier,
                3
            );
            assert_eq!(
                supervisor
                    .snapshot(b.id)
                    .expect("B4 consumed once")
                    .capture_attempt_frontier,
                4
            );
            let owner = supervisor.owner.as_mut().expect("owner");
            let marker_record = owner
                .watermarks()
                .written
                .expect("PreCut watermark")
                .checked_next()
                .expect("marker");
            let kind = match fault {
                EpochPersistenceFault::Persist => recording::SinkFaultKind::BeforeWrite(
                    session::PersistError::new("marker fault"),
                ),
                EpochPersistenceFault::ReceiptMismatch => {
                    recording::SinkFaultKind::ReceiptMismatch {
                        through: marker_record.checked_next().expect("mismatch"),
                    }
                }
                EpochPersistenceFault::InsufficientGate => recording::SinkFaultKind::WeakGate {
                    achieved: RecordingGate::Written,
                },
            };
            owner
                .set_sink_fault(
                    &mut supervisor.turn,
                    Some(recording::SinkFault {
                        at: marker_record,
                        kind,
                    }),
                )
                .expect("same writer negative fault");
            let failed = supervisor
                .inner
                .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
            assert!(failed.outcome.is_err());
            assert!(matches!(
                failed.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
            assert!(matches!(
                owner.session_status().marker,
                session::MarkerState::Unconfirmed(_)
            ));
            let fixed_ledger = supervisor.inner.retention_report();
            assert_eq!(fixed_ledger.ownership.pre_cut, 0);
            assert_eq!(fixed_ledger.ownership.post_cut, usize::from(free_work));
            assert_eq!(fixed_ledger.ownership.work_used, usize::from(free_work));
            assert_eq!(fixed_ledger.cut_remaining, Some(0));
            let b_failure = supervisor.inner.terminal_failure(b.id);
            let b_state = supervisor.snapshot(b.id).expect("fixed B state");
            for index in 0..16 {
                let repeated = supervisor.inner.queue_text(
                    &mut supervisor.turn,
                    b.connection_id,
                    b.tag.connection,
                    stamp(at + 50 + index),
                    &[b'x'; 65537],
                );
                assert_eq!(
                    repeated.outcome,
                    if free_work {
                        Err(SupervisorError::Halted)
                    } else {
                        Ok(AdmissionOutcome::AlreadyTerminated)
                    }
                );
                assert_eq!(repeated.failure, b_failure);
                assert!(repeated.commands.is_empty());
                assert_eq!(supervisor.snapshot(b.id).expect("no reuse/reset"), b_state);
                assert_eq!(
                    supervisor.inner.retention_report().ownership,
                    fixed_ledger.ownership
                );
                assert_eq!(supervisor.inner.retention_report().cut_remaining, Some(0));
            }
            let mut reader = WalReader::open(&supervisor.temp.path).expect("physical prefix");
            let mut b_gaps = Vec::new();
            while let Some(frame) = reader.next_record().expect("prefix") {
                if let Record::Gap(gap) = frame.value
                    && gap.reason == Reason::QueueOverflow
                    && q1_blocker_target(&gap).stream == b.id
                {
                    b_gaps.push(gap);
                }
            }
            assert_eq!(
                b_gaps.len(),
                1,
                "PostCut B4 cannot persist after failed marker"
            );
            let target = q1_blocker_target(&b_gaps[0]);
            assert_eq!(
                target.range.map(|(first, last)| (first.get(), last.get())),
                Some((2, 3))
            );
            assert_eq!(target.loss_count, Some(2));
            assert_eq!(b_gaps[0].context.monotonic_ns.get(), at + 30);
            assert_eq!(
                reader.report().status,
                recording::ArchiveStatus::ValidPrefixIncomplete
            );
        }
    }
}

#[test]
fn admitted_connected_and_pong_without_prior_down_record_historical_up_after_cut_without_revival() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 5,
    };
    let mut supervisor =
        canonical_supervisor(vec![binding.clone()], policy, RecordingGate::Durable);
    supervisor
        .start_commands()
        .expect("initial Connect dispatched");
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(1))
        .expect("first admitted Connected");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(2),
            b"pong".to_vec(),
        )
        .expect("admitted Pong");
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(3))
        .expect("second admitted Connected");
    assert_eq!(
        supervisor.queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(4),
            b"{".to_vec()
        ),
        Err(SupervisorError::QueueExhausted { stream: binding.id })
    );
    let terminal = supervisor.snapshot(binding.id).expect("terminal state");
    assert!(terminal.capture_terminated);
    assert_eq!(terminal.capture_attempt_frontier, 1);
    assert_eq!(terminal.accounted_attempt_frontier, 0);
    assert_eq!(terminal.transport, Transport::Unknown);
    assert_eq!(terminal.subscription, SubscriptionState::Degraded);
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 3);
    let mut reader = WalReader::open(&supervisor.temp.path).expect("pre-drain prefix");
    while let Some(frame) = reader.next_record().expect("prefix") {
        assert!(!matches!(
            frame.value,
            Record::Control(ControlRecord {
                value: Control::Transport {
                    value: Transport::Down,
                    ..
                },
                ..
            })
        ));
    }
    for expected_stamp in 1..=3 {
        let report = supervisor
            .inner
            .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
        assert!(matches!(
            report.session_disposition,
            session::SessionDisposition::DiagnosticOnly { .. }
        ));
        let result = report
            .outcome
            .expect("historical received observation remains drainable")
            .expect("one owner");
        assert_eq!(result.records.len(), 1);
        assert!(result.commands.is_empty());
        if expected_stamp == 2 {
            assert!(result.events.iter().any(|event| matches!(event,
                SupervisorEvent::PongRecorded { stream, .. } if *stream == binding.id)));
        } else {
            assert!(result.events.iter().any(|event| matches!(event,
                SupervisorEvent::TransportRecorded { stream, value: Transport::Up, .. } if *stream == binding.id)));
        }
        let record = result.records[0];
        let ledger = supervisor.inner.retention_report().ownership;
        assert_eq!(
            ledger.work_used,
            4 - expected_stamp as usize,
            "held result still retains its original W"
        );
        drop(result);
        assert_eq!(
            supervisor.inner.retention_report().ownership.work_used,
            3 - expected_stamp as usize
        );
        assert_eq!(
            supervisor
                .snapshot(binding.id)
                .expect("historical Up does not revive live capture"),
            terminal
        );
        let mut reader = WalReader::open(&supervisor.temp.path).expect("gate-confirmed prefix");
        let mut observed = None;
        while let Some(frame) = reader.next_record().expect("accepted frames") {
            if frame.record_no == record {
                observed = Some(frame);
            }
        }
        let frame = observed.expect("real WAL receipt confirms the original historical Up");
        let Record::Control(ControlRecord {
            context,
            value:
                Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Up,
                },
        }) = frame.value
        else {
            panic!("truthful historical Up")
        };
        assert_eq!(connection, binding.connection_id);
        assert_eq!(epoch, binding.tag.connection);
        assert_eq!(context.monotonic_ns.get(), expected_stamp);
        assert_eq!(context.unix_ns.get(), stamp(expected_stamp).unix_ns);
    }
    let marker = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
    assert!(matches!(
        marker.session_disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    drop(
        marker
            .outcome
            .expect("failure marker")
            .expect("reserved marker observation"),
    );
    let repeated_tick = supervisor
        .inner
        .queue_tick(&mut supervisor.turn, stamp(HEARTBEAT_INTERVAL_NS + 10));
    assert_eq!(repeated_tick.outcome, Ok(AdmissionOutcome::Admitted));
    assert!(repeated_tick.commands.is_empty());
    assert!(repeated_tick.admitted_scopes.iter().all(Option::is_none));
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 0);
}

#[test]
fn borrowed_tiny_slice_never_retains_the_callers_large_vector_capacity() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 16,
        max_raw_message_bytes: 16,
        max_total_items: 5,
    };
    let mut supervisor =
        canonical_supervisor(vec![binding.clone()], policy, RecordingGate::Flushed);
    supervisor.start_commands().expect("start");
    let mut incoming = Vec::with_capacity(8 * 1024 * 1024);
    incoming.push(b'{');
    assert!(incoming.capacity() > policy.max_raw_bytes_per_stream);
    let admitted = supervisor.inner.queue_text(
        &mut supervisor.turn,
        binding.connection_id,
        binding.tag.connection,
        stamp(1),
        &incoming,
    );
    assert_eq!(admitted.outcome, Ok(AdmissionOutcome::Admitted));
    assert!(admitted.commands.is_empty());
    let retention = supervisor.inner.retention_report();
    assert_eq!(retention.ownership.work_used, 1);
    assert_eq!(retention.raw[0].payload_bytes, 1);
    assert_eq!(retention.raw[0].allocated_bytes, 1);
    assert_eq!(retention.payload_ceiling_bytes, 16);
    assert!(retention.reported_backing_bytes <= retention.retained_bytes_ceiling);
    drop(incoming);
    let result = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("bounded decode scratch")
        .expect("one admitted owner");
    assert_eq!(
        supervisor.inner.retention_report().ownership.work_used,
        1,
        "held observation owns W"
    );
    drop(result);
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 0);
    assert_eq!(
        supervisor.inner.retention_report().raw[0].allocated_bytes,
        0
    );
}

#[test]
fn marker_precedes_post_cut_neighbor_raw_and_unadmitted_completion_with_held_close() {
    let old_a = stream_binding(1, 1, 1, "BTCUSDT");
    let b = stream_binding(2, 2, 2, "ETHUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 65536,
        max_raw_message_bytes: 65536,
        max_total_items: 9,
    };
    let mut supervisor = canonical_supervisor(
        vec![old_a.clone(), b.clone()],
        policy,
        RecordingGate::Durable,
    );
    let mut trace = MemorySink::default();
    supervisor.start_commands().expect("start");
    let a = q1_blocker_next_generation(&mut supervisor, &mut trace, &old_a, 1);
    let at = RECONNECT_MAX_NS_V1 + 100;
    connect_one(&mut supervisor, &mut trace, &b, at);
    let timer_at = at + HEARTBEAT_INTERVAL_NS;
    supervisor
        .queue_disconnected(b.connection_id, b.tag.connection, stamp(timer_at - 1))
        .expect("B Down admitted");
    supervisor
        .queue_tick(stamp(timer_at))
        .expect("pre-cut B timer admitted while transport still Up");
    let down = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("gate-confirmed Down")
        .expect("Down result");
    assert_eq!(down.commands.len(), 1);
    let mut down = down;
    let close = std::mem::take(&mut down.commands)
        .into_iter()
        .next()
        .expect("held B Close lease");
    let close_owner = close.close_owner().expect("transferred Down owner").clone();
    drop(down);
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 2);
    for index in 0..4 {
        supervisor
            .queue_text(
                a.connection_id,
                old_a.tag.connection,
                stamp(timer_at + 10 + index),
                vec![b'x'; 1_100_000],
            )
            .expect("separate PreCut A diagnostics");
    }
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 6);
    assert_eq!(
        supervisor.queue_text(
            a.connection_id,
            a.tag.connection,
            stamp(timer_at + 20),
            b"{".to_vec()
        ),
        Err(SupervisorError::QueueExhausted { stream: a.id })
    );
    let timer = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
    assert!(matches!(
        timer.session_disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    let timer = timer
        .outcome
        .expect("pre-cut timer gate")
        .expect("timer result");
    assert!(
        timer.commands.is_empty(),
        "historical timer cannot duplicate B Close or send Ping"
    );
    assert!(timer.events.iter().any(|event| matches!(event, SupervisorEvent::HeartbeatTimerRecorded { stream, .. } if *stream == b.id)));
    drop(timer);
    let post = supervisor.inner.queue_text(
        &mut supervisor.turn,
        b.connection_id,
        b.tag.connection,
        stamp(timer_at + 21),
        b"{",
    );
    assert_eq!(post.outcome, Ok(AdmissionOutcome::Admitted));
    assert!(matches!(
        post.session_disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 6);
    assert_eq!(supervisor.inner.retention_report().ownership.post_cut, 1);
    while supervisor.inner.retention_report().cut_remaining != Some(0) {
        let pre = supervisor
            .inner
            .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
        drop(
            pre.outcome
                .expect("remaining PreCut diagnostics")
                .expect("owner"),
        );
    }
    assert_eq!(
        supervisor.inner.retention_report().ownership.work_used,
        2,
        "held Down/Close plus post-cut Raw remain counted"
    );
    let owner = supervisor.owner.as_mut().expect("owner");
    assert!(matches!(
        owner.reclaim_close(&mut supervisor.turn, close_owner.clone()),
        session::CloseLeaseReport::AlreadyLeased
    ));
    let earlier_watermark = owner
        .watermarks()
        .durable
        .expect("trusted stronger earlier receipt");
    let marker = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink);
    assert!(matches!(
        marker.session_disposition,
        session::SessionDisposition::DiagnosticOnly { .. }
    ));
    let marker = marker
        .outcome
        .expect("marker cannot wait on unadmitted completion")
        .expect("marker observation");
    let marker_no = marker.records[0];
    drop(marker);
    assert_eq!(
        owner.session_status().marker,
        session::MarkerState::Confirmed(marker_no)
    );
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 2);
    let raw = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("post-cut Raw drains after marker")
        .expect("Raw result");
    assert!(raw.records[0] > marker_no);
    assert_eq!(
        supervisor
            .inner
            .snapshot(b.id)
            .expect("Close still held")
            .tag,
        b.tag
    );
    assert!(raw.commands.is_empty());
    drop(raw);
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 1);
    assert!(
        supervisor
            .inner
            .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
            .outcome
            .expect("held Close wait")
            .is_none()
    );
    assert!(matches!(
        owner.dispatch(&mut supervisor.turn, close, |_| Ok::<(), ()>(())),
        session::DispatchReport::Dispatched
    ));
    assert_eq!(
        supervisor.inner.retention_report().ownership.work_used,
        1,
        "Close settlement alone does not release the plan"
    );
    let completion = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("unadmitted generated completion follows marker and raw")
        .expect("completion");
    assert_eq!(completion.records.len(), 3);
    assert!(completion.records.iter().all(|record| *record > marker_no));
    assert_eq!(completion.commands.len(), 1);
    let mut completion = completion;
    let reconnect = std::mem::take(&mut completion.commands)
        .into_iter()
        .next()
        .expect("B remains serviceable");
    assert!(matches!(
        reconnect.kind(),
        session::CommandKind::ReconnectAfter { .. }
    ));
    assert!(matches!(
        owner.dispatch(&mut supervisor.turn, reconnect, |_| Ok::<(), ()>(())),
        session::DispatchReport::Dispatched
    ));
    drop(completion);
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 0);
    assert!(owner.session_status().failed);
    assert!(matches!(
        owner.begin_finalization(&mut supervisor.turn),
        Err(recording::OwnerError::Authority(
            session::AuthorityError::ArchiveFailed
        ))
    ));
    let mut reader = WalReader::open(&supervisor.temp.path).expect("physical ordering");
    let mut marker_found = false;
    let mut post_found = false;
    while let Some(frame) = reader.next_record().expect("accepted prefix") {
        if frame.record_no == marker_no {
            let Record::Control(ControlRecord {
                value: Control::Recording(evidence),
                ..
            }) = frame.value
            else {
                panic!("marker")
            };
            assert_eq!(evidence.through, Some(earlier_watermark));
            assert!(evidence.through.expect("earlier") < marker_no);
            assert_eq!(evidence.kind, WatermarkKind::Durable);
            marker_found = true;
        } else if let Record::RawInput(raw) = frame.value
            && raw.stream == b.id
        {
            assert!(marker_found, "post-cut B input cannot move before marker");
            assert_eq!(raw.context.monotonic_ns.get(), timer_at + 21);
            assert_eq!(raw.attempt.get(), 1);
            post_found = true;
        }
    }
    assert!(marker_found && post_found);
}

#[test]
fn full_supervisor_requested_heap_retention_and_decode_peak_fit_cap5_and_cap9_profile() {
    // Caller-owned input capacities and the external path predate the probe and
    // stay alive until it ends. Every session-owned allocation is created and
    // released within the observed window; the measurement is requested heap
    // bytes rather than allocator bookkeeping or process RSS.
    for (n, cap) in [(1, 5), (2, 9)] {
        let wal = TempWal::new("full-supervisor-allocation");
        let mut tiny_large_capacity = Vec::with_capacity(8 * 1024 * 1024);
        tiny_large_capacity.push(b'{');
        let mut dense = ack("BTCUSDT");
        dense.pop();
        dense.extend_from_slice(b",\"padding\":");
        dense.extend(std::iter::repeat_n(b'[', 10));
        dense.push(b'[');
        for index in 0..200 {
            if index != 0 {
                dense.push(b',');
            }
            dense.push(b'0');
        }
        dense.push(b']');
        dense.extend(std::iter::repeat_n(b']', 10));
        dense.push(b'}');
        assert!(dense.len() < 4096);
        dense.resize(4096, b' ');
        let probe = AllocationProbe::begin();
        let advertised_ceiling;
        {
            let mut bindings = vec![stream_binding(1, 1, 1, "BTCUSDT")];
            if n == 2 {
                bindings.push(stream_binding(2, 2, 2, "ETHUSDT"));
            }
            let binding = bindings[0].clone();
            let policy = QueuePolicy {
                max_raw_frames_per_stream: 1,
                max_raw_bytes_per_stream: 4096,
                max_raw_message_bytes: 4096,
                max_total_items: cap,
            };
            let prefix = bootstrap_prefix_many(&bindings, RecordingGate::Flushed);
            let next_record = prefix
                .last()
                .expect("prefix")
                .record_no
                .checked_next()
                .expect("next");
            let (mut owner, mut turn) = CaptureSessionOwner::create_new(
                &wal.path,
                &prefix[0],
                BoundedCaptureProfile::new(&prefix[1..]),
            )
            .expect("fresh accepted owner");
            let scopes: Vec<_> = bindings
                .iter()
                .map(|binding| session::ScopeBinding {
                    stream: binding.id,
                    connection: binding.connection_id,
                    epoch: binding.tag.connection,
                })
                .collect();
            let (handle, mut sink) = owner
                .register_supervisor(
                    &mut turn,
                    &scopes,
                    session::RetentionBudget {
                        item_cap: cap,
                        raw_frame_limit: 1,
                        raw_byte_limit: 4096,
                        max_message_bytes: 4096,
                    },
                )
                .expect("register fixed full scope set");
            let mut supervisor = BoundSupervisor::new(
                supervisor_config_with_record(
                    bindings,
                    policy,
                    RecordingGate::Flushed,
                    next_record,
                ),
                handle,
            )
            .expect("canonical supervisor");
            advertised_ceiling = supervisor.retention_report().retained_bytes_ceiling;
            let check = |supervisor: &BoundSupervisor, probe: &AllocationProbe| {
                let report = supervisor.retention_report();
                assert!(
                    report.ownership.work_used
                        + report.ownership.reserved_scopes
                        + report.ownership.reserved_archive
                        <= cap
                );
                assert_eq!(report.ownership.work_limit + n + 1, cap);
                assert!(report.reported_backing_bytes <= report.retained_bytes_ceiling);
                let sample = probe.sample();
                assert!(!sample.unmatched_deallocation);
                assert!(sample.live_requested_bytes <= advertised_ceiling);
                assert!(sample.peak_requested_bytes <= advertised_ceiling);
            };
            check(&supervisor, &probe);
            let started = supervisor.start_commands(&mut turn);
            assert_eq!(started.outcome, Ok(AdmissionOutcome::Admitted));
            assert_eq!(supervisor.retention_report().ownership.work_used, n);
            check(&supervisor, &probe);
            for command in started.commands {
                assert!(matches!(
                    owner.dispatch(&mut turn, command, |_| Ok::<(), ()>(())),
                    session::DispatchReport::Dispatched
                ));
            }
            assert_eq!(supervisor.retention_report().ownership.work_used, 0);
            supervisor
                .queue_connected(
                    &mut turn,
                    binding.connection_id,
                    binding.tag.connection,
                    stamp(1),
                )
                .outcome
                .expect("Connected");
            let mut up = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("Up gate")
                .expect("Up result");
            assert_eq!(up.commands.len(), 1);
            assert_eq!(supervisor.retention_report().ownership.work_used, 1);
            check(&supervisor, &probe);
            let subscribe = std::mem::take(&mut up.commands)
                .into_iter()
                .next()
                .expect("Subscribe lease");
            drop(up);
            assert_eq!(
                supervisor.retention_report().ownership.work_used,
                1,
                "held CommandLease owns W"
            );
            assert!(matches!(
                owner.dispatch(&mut turn, subscribe, |_| Ok::<(), ()>(())),
                session::DispatchReport::Dispatched
            ));
            assert_eq!(supervisor.retention_report().ownership.work_used, 0);
            supervisor
                .queue_text(
                    &mut turn,
                    binding.connection_id,
                    binding.tag.connection,
                    stamp(2),
                    &dense,
                )
                .outcome
                .expect("legal dense/nested JSON at message bound");
            assert_eq!(supervisor.retention_report().raw[0].allocated_bytes, 4096);
            check(&supervisor, &probe);
            let dense_result = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("bounded parser/encoder scratch")
                .expect("dense result");
            assert!(
                dense_result
                    .events
                    .iter()
                    .any(|event| matches!(event, SupervisorEvent::SubscriptionAccepted { .. })),
                "the nested JSON is legal and reaches the accepted decoder path"
            );
            assert_eq!(supervisor.retention_report().ownership.work_used, 1);
            check(&supervisor, &probe);
            drop(dense_result);
            supervisor
                .queue_text(
                    &mut turn,
                    binding.connection_id,
                    binding.tag.connection,
                    stamp(3),
                    &tiny_large_capacity,
                )
                .outcome
                .expect("borrowed tiny slice");
            assert_eq!(supervisor.retention_report().raw[0].allocated_bytes, 1);
            let tiny = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("tiny diagnostic decode")
                .expect("tiny result");
            check(&supervisor, &probe);
            drop(tiny);
            let work_limit = supervisor.retention_report().ownership.work_limit;
            for index in 0..work_limit {
                supervisor
                    .queue_connected(
                        &mut turn,
                        binding.connection_id,
                        binding.tag.connection,
                        stamp(10 + index as u64),
                    )
                    .outcome
                    .expect("counted controls");
                check(&supervisor, &probe);
            }
            let failure = supervisor.queue_text(
                &mut turn,
                binding.connection_id,
                binding.tag.connection,
                stamp(30),
                &tiny_large_capacity,
            );
            assert_eq!(
                failure.outcome,
                Err(SupervisorError::QueueExhausted { stream: binding.id })
            );
            let close_ref = failure.close_owner.expect("reserved terminal Close");
            assert_eq!(
                supervisor.retention_report().ownership.work_used,
                work_limit
            );
            check(&supervisor, &probe);
            drop(failure.commands);
            let stable_bytes = probe.sample().live_requested_bytes;
            let stable_ledger = supervisor.retention_report().ownership;
            for index in 0..100 {
                let repeated = supervisor.queue_text(
                    &mut turn,
                    binding.connection_id,
                    binding.tag.connection,
                    stamp(40 + index),
                    &dense,
                );
                assert_eq!(repeated.outcome, Ok(AdmissionOutcome::AlreadyTerminated));
                assert!(repeated.commands.is_empty());
                let session::CloseLeaseReport::Leased(lease) =
                    owner.reclaim_close(&mut turn, close_ref.clone())
                else {
                    panic!("same reserved Close is reclaimable")
                };
                drop(lease);
                assert_eq!(supervisor.retention_report().ownership, stable_ledger);
                assert_eq!(
                    probe.sample().live_requested_bytes,
                    stable_bytes,
                    "repeats/reclaim retain no new allocation"
                );
                check(&supervisor, &probe);
            }
            while let Some(result) = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("diagnostic drain")
            {
                check(&supervisor, &probe);
                drop(result);
            }
            assert_eq!(supervisor.retention_report().ownership.work_used, 0);
            let session::CloseLeaseReport::Leased(lease) =
                owner.reclaim_close(&mut turn, close_ref.clone())
            else {
                panic!("mandatory Close")
            };
            assert!(matches!(
                owner.dispatch(&mut turn, lease.into_command(), |_| Ok::<(), ()>(())),
                session::DispatchReport::Dispatched
            ));
            assert!(matches!(
                owner.close_diagnostic(&mut turn).outcome,
                Ok(recording::DiagnosticCloseState::Closed)
            ));
            check(&supervisor, &probe);
            drop(close_ref);
            drop(supervisor);
            drop(sink);
            drop(turn);
            drop(owner);
        }
        let released = probe.sample();
        assert!(!released.unmatched_deallocation);
        assert_eq!(
            released.live_requested_bytes, 0,
            "all session and temporary storage is released"
        );
        assert!(released.peak_requested_bytes <= advertised_ceiling);
        drop(probe);
        eprintln!(
            "supervisor allocation profile: N={n} M={cap} P=4096 peak={} requested_bytes ceiling={advertised_ceiling} released={}",
            released.peak_requested_bytes, released.live_requested_bytes
        );
        drop(dense);
        drop(tiny_large_capacity);
    }
}

#[test]
fn held_pre_failure_connect_lease_counts_toward_cap_and_is_revoked_before_effect() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let policy = QueuePolicy {
        max_raw_frames_per_stream: 1,
        max_raw_bytes_per_stream: 4096,
        max_raw_message_bytes: 4096,
        max_total_items: 5,
    };
    let mut supervisor =
        canonical_supervisor(vec![binding.clone()], policy, RecordingGate::Flushed);
    let started = supervisor.inner.start_commands(&mut supervisor.turn);
    assert_eq!(started.outcome, Ok(AdmissionOutcome::Admitted));
    let stale = started
        .commands
        .into_iter()
        .next()
        .expect("held Connect lease");
    assert!(matches!(stale.kind(), session::CommandKind::Connect { .. }));
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 1);
    supervisor
        .queue_connected(binding.connection_id, binding.tag.connection, stamp(1))
        .expect("admitted Connected");
    supervisor
        .queue_text(
            binding.connection_id,
            binding.tag.connection,
            stamp(2),
            b"pong".to_vec(),
        )
        .expect("admitted Pong");
    assert_eq!(
        supervisor.inner.retention_report().ownership.work_used,
        3,
        "a held pre-failure lease cannot disappear from advertised capacity"
    );
    let failure = supervisor.inner.queue_text(
        &mut supervisor.turn,
        binding.connection_id,
        binding.tag.connection,
        stamp(3),
        b"{",
    );
    assert_eq!(
        failure.outcome,
        Err(SupervisorError::QueueExhausted { stream: binding.id })
    );
    assert_eq!(failure.commands.len(), 1);
    let owner = supervisor.owner.as_mut().expect("owner");
    assert!(matches!(
        owner.dispatch(&mut supervisor.turn, stale, |_| -> Result<(), ()> {
            panic!("old Connect cannot execute after irreversible scope failure")
        }),
        session::DispatchReport::Revoked(session::AuthorityError::CommandRevoked)
    ));
    assert_eq!(
        supervisor.inner.retention_report().ownership.work_used,
        2,
        "revoked nonmandatory lease releases only its own W"
    );
    for close in failure.commands {
        assert!(matches!(
            owner.dispatch(&mut supervisor.turn, close, |_| Ok::<(), ()>(())),
            session::DispatchReport::Dispatched
        ));
    }
    while let Some(result) = supervisor
        .inner
        .drain_one(&mut supervisor.turn, &mut supervisor.bound_sink)
        .outcome
        .expect("admitted diagnostic prefix")
    {
        assert!(result.commands.is_empty());
        drop(result);
    }
    assert_eq!(supervisor.inner.retention_report().ownership.work_used, 0);
    assert!(matches!(
        owner.close_diagnostic(&mut supervisor.turn).outcome,
        Ok(recording::DiagnosticCloseState::Closed)
    ));
}
