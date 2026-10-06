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

fn supervisor_config(
    streams: Vec<StreamBinding>,
    queue_policy: QueuePolicy,
    gate: RecordingGate,
) -> WsSupervisorConfig {
    WsSupervisorConfig {
        active_context: active_context(),
        recording_gate: gate,
        segment_no: SegmentNo::new(0),
        next_record_no: id(RecordNo::new(5)),
        queue_policy,
        streams,
    }
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
    assert_eq!(loss.records.len(), 4);
    assert!(
        loss.events
            .iter()
            .any(|event| matches!(event, SupervisorEvent::QueueGapRecorded { .. }))
    );
    assert!(loss.events.iter().any(|event| matches!(
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
    assert_eq!(disconnected.records.len(), 4);
    let current = supervisor.snapshot(stream).expect("snapshot");
    assert_eq!(current.transport, Transport::Unknown);
    assert_eq!(current.subscription, SubscriptionState::Backoff);
    assert_eq!(current.tag.connection.get(), 2);
    assert_eq!(current.tag.subscription.get(), 2);
    assert_eq!(current.tag.book.expect("book epoch").get(), 2);

    let (delay, next_epoch) = disconnected
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
    assert_eq!(
        first
            .commands
            .iter()
            .chain(second.commands.iter())
            .filter(|command| matches!(command, TransportCommand::ReconnectAfter { .. }))
            .count(),
        1
    );
    assert_eq!(
        second
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
    assert_eq!(
        timed_out
            .commands
            .iter()
            .chain(duplicate.commands.iter())
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
    assert!(duplicate.events.iter().any(|event| matches!(
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
    assert!(last.events.iter().any(|event| matches!(
        event,
        SupervisorEvent::EpochAdvanced { tag, .. } if tag.connection.get() == 2
    )));
    assert!(last.commands.iter().any(|command| matches!(
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
    assert!(first.events.iter().any(|event| matches!(
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
    supervisor.drain_one(&mut sink).expect("disconnect drain");
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
    assert_eq!(result.records.len(), 5);
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
