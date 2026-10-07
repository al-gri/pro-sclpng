use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use domain::artifact::ArtifactRef;
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, PolicyFields, RecordingGate, SilenceRule};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;
use market_data::*;
use recording::{WalReader, WalWriter};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

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
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
        self.frames.push(frame.clone());
        Ok(PersistenceReceipt {
            through: frame.record_no,
            achieved: self.achieved.unwrap_or(required_gate),
        })
    }
}

struct WriterSink {
    writer: WalWriter,
}

impl RecordSink for WriterSink {
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
        let mut marks = self
            .writer
            .append(frame)
            .map_err(|error| PersistError::new(error.to_string()))?;
        let through = match required_gate {
            RecordingGate::Written => marks.written,
            RecordingGate::Flushed => {
                marks = self
                    .writer
                    .flush()
                    .map_err(|error| PersistError::new(error.to_string()))?;
                marks.flushed
            }
            RecordingGate::Durable => {
                marks = self
                    .writer
                    .sync_all()
                    .map_err(|error| PersistError::new(error.to_string()))?;
                marks.durable
            }
        }
        .ok_or_else(|| PersistError::new("required storage frontier did not advance"))?;
        Ok(PersistenceReceipt {
            through,
            achieved: required_gate,
        })
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

    assert!(matches!(
        PublicWsSupervisor::new(supervisor_config(
            vec![first, second],
            QueuePolicy::default(),
            RecordingGate::Written,
        )),
        Err(SupervisorError::Identity(
            IdentityError::WriterRebindRequiresNewArchive
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
fn max_record_frontier_terminal_disconnect_halts_without_partial_down() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let config = supervisor_config_with_record(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
        id(RecordNo::new(u64::MAX - 1)),
    );
    let mut supervisor = PublicWsSupervisor::new(config).expect("supervisor");
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    assert_eq!(sink.frames.len(), 1);
    assert_eq!(sink.frames[0].record_no.get(), u64::MAX - 1);

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(2))
        .expect("queue down");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::CounterExhausted("RecordNo"))
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), 1);
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::Halted)
    );
    assert_eq!(sink.frames.len(), 1);
}

#[test]
fn insufficient_record_capacity_for_full_disconnect_halts_before_down() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let config = supervisor_config_with_record(
        vec![binding.clone()],
        QueuePolicy::default(),
        RecordingGate::Written,
        id(RecordNo::new(u64::MAX - 4)),
    );
    let mut supervisor = PublicWsSupervisor::new(config).expect("supervisor");
    let mut sink = MemorySink::default();
    supervisor.start_commands().expect("start");
    connect_one(&mut supervisor, &mut sink, &binding, 1);
    assert_eq!(sink.frames[0].record_no.get(), u64::MAX - 4);
    let before = sink.frames.len();

    supervisor
        .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(2))
        .expect("queue down");
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::CounterExhausted("RecordNo"))
    );
    assert!(supervisor.is_halted());
    assert_eq!(sink.frames.len(), before);
    assert_eq!(
        supervisor.drain_one(&mut sink),
        Err(SupervisorError::Halted)
    );
}

#[test]
fn accepted_wal_writer_preserves_definition_control_and_raw_order_offline() {
    let binding = stream_binding(1, 1, 1, "BTCUSDT");
    let temp = TempWal::new("accepted-path");
    let mut writer = WalWriter::create(&temp.path).expect("create wal");

    let prefix = bootstrap_prefix(&binding, RecordingGate::Flushed);
    for frame in &prefix {
        writer.append(frame).expect("append prefix");
        writer.flush().expect("flush prefix");
    }

    let mut sink = WriterSink { writer };
    let mut supervisor = PublicWsSupervisor::new(WsSupervisorConfig {
        active_context: active_context(),
        recording_gate: RecordingGate::Flushed,
        segment_no: SegmentNo::new(0),
        next_record_no: id(RecordNo::new(5)),
        queue_policy: QueuePolicy::default(),
        streams: vec![binding.clone()],
    })
    .expect("supervisor");
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

    let mut reader = WalReader::open(&temp.path).expect("open wal");
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
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
        self.attempts.push((frame.clone(), required_gate));
        if matches!(
            &frame.value,
            Record::Control(ControlRecord {
                value: Control::EpochAdvance { .. },
                ..
            })
        ) {
            self.epoch_attempts += 1;
            if self.epoch_attempts == self.fault_at {
                // Receiving a frame is only an attempted write: an error or an
                // invalid/weak receipt never confirms it at the required gate.
                return match self.fault {
                    EpochPersistenceFault::Persist => {
                        Err(PersistError::new("injected epoch persistence error"))
                    }
                    EpochPersistenceFault::ReceiptMismatch => Ok(PersistenceReceipt {
                        through: frame.record_no.checked_next().expect("test receipt number"),
                        achieved: required_gate,
                    }),
                    EpochPersistenceFault::InsufficientGate => Ok(PersistenceReceipt {
                        through: frame.record_no,
                        achieved: RecordingGate::Written,
                    }),
                };
            }
        }
        self.confirmed.push(frame.clone());
        Ok(PersistenceReceipt {
            through: frame.record_no,
            achieved: required_gate,
        })
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
