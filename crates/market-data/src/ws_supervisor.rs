use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

use domain::event::{ActiveContext, InputContext};
use domain::identity::{
    CaptureAttemptNo, Channel, ConnectionEpoch, ConnectionId, EpochTag, IdentityError, LocalUnixNs,
    MarketKind, MonotonicNs, RecordNo, SegmentNo, StreamBinding, StreamId,
};
use domain::policy::RecordingGate;
use domain::record::{
    Control, ControlRecord, EpochChange, Gap, GapScope, GapTarget, RawInput, Reason, Record,
    RecordFrame, Transport, WireContext,
};

use crate::json::{JsonValue, ParserLimits, parse_json};
use crate::{
    BitgetMessage, Category, ContinuityClassifier, ContinuityOutcome, DecodeLimits, Topic,
    decode_message_with_limits,
};

pub const BITGET_PUBLIC_WS_ENDPOINT: &str = "wss://ws.bitget.com/v3/ws/public";
pub const SUPERVISOR_POLICY_VERSION: u32 = 1;
pub const MAX_CONFIGURED_STREAMS: usize = 4;
pub const HEARTBEAT_INTERVAL_NS: u64 = 30_000_000_000;
pub const PONG_TIMEOUT_NS_V1: u64 = 15_000_000_000;
pub const RECONNECT_BASE_NS_V1: u64 = 6_000_000_000;
pub const RECONNECT_MAX_NS_V1: u64 = 60_000_000_000;

const HARD_MAX_RAW_FRAMES_PER_STREAM: usize = 64;
const HARD_MAX_RAW_BYTES_PER_STREAM: usize = 1_000_000;
const HARD_MAX_RAW_MESSAGE_BYTES: usize = 1_000_000;
const HARD_MAX_TOTAL_ITEMS: usize = 256;
const CONTROL_RESERVE_PER_STREAM: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveStamp {
    pub unix_ns: i64,
    pub monotonic_ns: u64,
}

impl ReceiveStamp {
    fn wire_context(self, active: ActiveContext) -> WireContext {
        WireContext {
            unix_ns: LocalUnixNs::new(self.unix_ns),
            monotonic_ns: MonotonicNs::new(self.monotonic_ns),
            context: InputContext::Active(active),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuePolicy {
    pub max_raw_frames_per_stream: usize,
    pub max_raw_bytes_per_stream: usize,
    pub max_raw_message_bytes: usize,
    pub max_total_items: usize,
}

impl QueuePolicy {
    pub const fn conservative_v1() -> Self {
        Self {
            max_raw_frames_per_stream: 8,
            max_raw_bytes_per_stream: 512 * 1024,
            max_raw_message_bytes: 64 * 1024,
            max_total_items: 64,
        }
    }

    fn validate(self, stream_count: usize) -> Result<Self, SupervisorError> {
        let control_reserve = stream_count.checked_mul(CONTROL_RESERVE_PER_STREAM).ok_or(
            SupervisorError::InvalidConfiguration("control reserve overflow"),
        )?;
        if !(1..=HARD_MAX_RAW_FRAMES_PER_STREAM).contains(&self.max_raw_frames_per_stream) {
            return Err(SupervisorError::InvalidConfiguration(
                "max_raw_frames_per_stream",
            ));
        }
        if !(1..=HARD_MAX_RAW_BYTES_PER_STREAM).contains(&self.max_raw_bytes_per_stream) {
            return Err(SupervisorError::InvalidConfiguration(
                "max_raw_bytes_per_stream",
            ));
        }
        if !(1..=HARD_MAX_RAW_MESSAGE_BYTES).contains(&self.max_raw_message_bytes)
            || self.max_raw_message_bytes > self.max_raw_bytes_per_stream
        {
            return Err(SupervisorError::InvalidConfiguration(
                "max_raw_message_bytes",
            ));
        }
        if self.max_total_items > HARD_MAX_TOTAL_ITEMS || self.max_total_items <= control_reserve {
            return Err(SupervisorError::InvalidConfiguration("max_total_items"));
        }
        Ok(self)
    }
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self::conservative_v1()
    }
}

#[derive(Clone, Debug)]
pub struct WsSupervisorConfig {
    pub active_context: ActiveContext,
    pub recording_gate: RecordingGate,
    pub segment_no: SegmentNo,
    pub next_record_no: RecordNo,
    pub queue_policy: QueuePolicy,
    pub streams: Vec<StreamBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistenceReceipt {
    pub through: RecordNo,
    pub achieved: RecordingGate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistError {
    message: String,
}

impl PersistError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for PersistError {}

pub trait RecordSink {
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupervisorError {
    Identity(IdentityError),
    InvalidConfiguration(&'static str),
    AlreadyStarted,
    NotStarted,
    UnknownConnection(ConnectionId),
    UnknownConnectionEpoch {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
    },
    ReconnectTooEarly {
        connection: ConnectionId,
        not_before_ns: u64,
        observed_ns: u64,
    },
    QueueExhausted {
        stream: StreamId,
    },
    CounterExhausted(&'static str),
    TimeOverflow,
    Persistence(PersistError),
    PersistenceReceiptMismatch {
        expected: RecordNo,
        actual: RecordNo,
    },
    PersistenceGateTooWeak {
        required: RecordingGate,
        achieved: RecordingGate,
    },
    Halted,
}

impl From<IdentityError> for SupervisorError {
    fn from(value: IdentityError) -> Self {
        Self::Identity(value)
    }
}

impl fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for SupervisorError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionState {
    Disconnected,
    Connecting,
    AwaitingAck,
    AwaitingSnapshot,
    CapturingRaw,
    Backoff,
    Degraded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamSupervisorSnapshot {
    pub stream: StreamId,
    pub connection: ConnectionId,
    pub tag: EpochTag,
    pub transport: Transport,
    pub subscription: SubscriptionState,
    pub capture_attempt_frontier: u64,
    pub queued_raw_frames: usize,
    pub queued_raw_bytes: usize,
    pub last_market_record: Option<RecordNo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportCommand {
    Connect {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        endpoint: &'static str,
    },
    SendText {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        text: String,
    },
    Close {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
    },
    ReconnectAfter {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        delay_ns: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupervisorEvent {
    TransportRecorded {
        stream: StreamId,
        record: RecordNo,
        value: Transport,
    },
    SubscriptionAccepted {
        stream: StreamId,
        record: RecordNo,
    },
    SubscriptionFailed {
        stream: StreamId,
        record: RecordNo,
    },
    RawBookRecorded {
        stream: StreamId,
        record: RecordNo,
        attempt: CaptureAttemptNo,
        continuity: ContinuityOutcome,
    },
    QueueGapRecorded {
        stream: StreamId,
        record: RecordNo,
        lost_attempt: CaptureAttemptNo,
    },
    SourceGapRecorded {
        stream: StreamId,
        record: RecordNo,
        reason: Reason,
    },
    ObsoleteRawRecorded {
        stream: StreamId,
        record: RecordNo,
        tag: EpochTag,
    },
    PongRecorded {
        stream: StreamId,
        record: RecordNo,
    },
    HeartbeatTimerRecorded {
        stream: StreamId,
        record: RecordNo,
        timer_id: u64,
    },
    EpochAdvanced {
        stream: StreamId,
        records: Vec<RecordNo>,
        tag: EpochTag,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrainResult {
    pub records: Vec<RecordNo>,
    pub commands: Vec<TransportCommand>,
    pub events: Vec<SupervisorEvent>,
}

struct StreamRuntime {
    binding: StreamBinding,
    previous_tag: Option<EpochTag>,
    transport: Transport,
    subscription: SubscriptionState,
    continuity: ContinuityClassifier,
    capture_attempt_frontier: u64,
    queued_raw_frames: usize,
    queued_raw_bytes: usize,
    reconnect_failures: u32,
    reconnect_not_before_ns: Option<u64>,
    next_ping_due_ns: Option<u64>,
    pong_deadline_ns: Option<u64>,
    ping_timer_queued: bool,
    pong_timeout_queued: bool,
    timer_frontier: u64,
    last_market_record: Option<RecordNo>,
}

impl StreamRuntime {
    fn snapshot(&self) -> StreamSupervisorSnapshot {
        StreamSupervisorSnapshot {
            stream: self.binding.id,
            connection: self.binding.connection_id,
            tag: self.binding.tag,
            transport: self.transport,
            subscription: self.subscription,
            capture_attempt_frontier: self.capture_attempt_frontier,
            queued_raw_frames: self.queued_raw_frames,
            queued_raw_bytes: self.queued_raw_bytes,
            last_market_record: self.last_market_record,
        }
    }

    fn tag_for_observed_connection(&self, epoch: ConnectionEpoch) -> Option<(EpochTag, bool)> {
        if self.binding.tag.connection == epoch {
            return Some((self.binding.tag, true));
        }
        self.previous_tag
            .filter(|tag| tag.connection == epoch)
            .map(|tag| (tag, false))
    }
}

enum Ingress {
    Connected {
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    },
    Disconnected {
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    },
    Raw {
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
        bytes: Vec<u8>,
        counts_toward_bounds: bool,
    },
    QueueGap {
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
    },
    Pong {
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    },
    PingTimer {
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        timer_id: u64,
        deadline_ns: u64,
    },
    PongTimeout {
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        timer_id: u64,
        deadline_ns: u64,
    },
}

#[derive(Clone, Copy)]
struct GapLoss {
    range: Option<(CaptureAttemptNo, CaptureAttemptNo)>,
    loss_count: Option<u64>,
}

pub struct PublicWsSupervisor {
    active_context: ActiveContext,
    recording_gate: RecordingGate,
    segment_no: SegmentNo,
    next_record_no: Option<RecordNo>,
    queue_policy: QueuePolicy,
    raw_item_limit: usize,
    streams: BTreeMap<StreamId, StreamRuntime>,
    by_connection: BTreeMap<ConnectionId, StreamId>,
    queue: VecDeque<Ingress>,
    started: bool,
    halted: bool,
}

impl PublicWsSupervisor {
    pub fn new(config: WsSupervisorConfig) -> Result<Self, SupervisorError> {
        if config.streams.is_empty() || config.streams.len() > MAX_CONFIGURED_STREAMS {
            return Err(SupervisorError::InvalidConfiguration("streams"));
        }
        let queue_policy = config.queue_policy.validate(config.streams.len())?;
        let control_reserve = config
            .streams
            .len()
            .checked_mul(CONTROL_RESERVE_PER_STREAM)
            .ok_or(SupervisorError::InvalidConfiguration(
                "control reserve overflow",
            ))?;
        let raw_item_limit = queue_policy
            .max_total_items
            .checked_sub(control_reserve)
            .ok_or(SupervisorError::InvalidConfiguration("max_total_items"))?;

        let mut stream_ids = BTreeSet::new();
        let mut connection_ids = BTreeSet::new();
        let mut book_ids = BTreeSet::new();
        let mut streams = BTreeMap::new();
        let mut by_connection = BTreeMap::new();

        for binding in config.streams {
            binding.validate()?;
            if binding.channel != Channel::BookNormal
                || !matches!(
                    binding.spec.instrument.market,
                    MarketKind::Perpetual | MarketKind::DatedFuture
                )
                || binding.book_id.is_none()
                || binding.tag.book.is_none()
            {
                return Err(SupervisorError::InvalidConfiguration(
                    "regular usdt-futures books50 binding",
                ));
            }
            if !stream_ids.insert(binding.id)
                || !connection_ids.insert(binding.connection_id)
                || !book_ids.insert(binding.book_id)
            {
                return Err(SupervisorError::InvalidConfiguration(
                    "one unique connection and book owner per stream",
                ));
            }
            by_connection.insert(binding.connection_id, binding.id);
            streams.insert(
                binding.id,
                StreamRuntime {
                    binding,
                    previous_tag: None,
                    transport: Transport::Unknown,
                    subscription: SubscriptionState::Disconnected,
                    continuity: ContinuityClassifier::new(),
                    capture_attempt_frontier: 0,
                    queued_raw_frames: 0,
                    queued_raw_bytes: 0,
                    reconnect_failures: 0,
                    reconnect_not_before_ns: None,
                    next_ping_due_ns: None,
                    pong_deadline_ns: None,
                    ping_timer_queued: false,
                    pong_timeout_queued: false,
                    timer_frontier: 0,
                    last_market_record: None,
                },
            );
        }

        Ok(Self {
            active_context: config.active_context,
            recording_gate: config.recording_gate,
            segment_no: config.segment_no,
            next_record_no: Some(config.next_record_no),
            queue_policy,
            raw_item_limit,
            streams,
            by_connection,
            queue: VecDeque::new(),
            started: false,
            halted: false,
        })
    }

    pub fn start_commands(&mut self) -> Result<Vec<TransportCommand>, SupervisorError> {
        self.ensure_running()?;
        if self.started {
            return Err(SupervisorError::AlreadyStarted);
        }
        self.started = true;
        let mut commands = Vec::with_capacity(self.streams.len());
        for runtime in self.streams.values_mut() {
            runtime.subscription = SubscriptionState::Connecting;
            commands.push(TransportCommand::Connect {
                connection: runtime.binding.connection_id,
                epoch: runtime.binding.tag.connection,
                endpoint: BITGET_PUBLIC_WS_ENDPOINT,
            });
        }
        Ok(commands)
    }

    pub fn snapshot(&self, stream: StreamId) -> Option<StreamSupervisorSnapshot> {
        self.streams.get(&stream).map(StreamRuntime::snapshot)
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }

    pub fn queued_items(&self) -> usize {
        self.queue.len()
    }

    pub fn queue_connected(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> Result<(), SupervisorError> {
        self.ensure_started()?;
        let stream = self.stream_for_connection(connection)?;
        let runtime = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::UnknownConnection(connection))?;
        if runtime.binding.tag.connection != epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }
        if let Some(not_before_ns) = runtime.reconnect_not_before_ns
            && stamp.monotonic_ns < not_before_ns
        {
            return Err(SupervisorError::ReconnectTooEarly {
                connection,
                not_before_ns,
                observed_ns: stamp.monotonic_ns,
            });
        }
        self.push_ingress(
            stream,
            Ingress::Connected {
                stream,
                epoch,
                stamp,
            },
        )
    }

    pub fn queue_disconnected(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> Result<(), SupervisorError> {
        self.ensure_started()?;
        let stream = self.stream_for_connection(connection)?;
        let current = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::UnknownConnection(connection))?
            .binding
            .tag
            .connection;
        if current != epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }
        self.push_ingress(
            stream,
            Ingress::Disconnected {
                stream,
                epoch,
                stamp,
            },
        )
    }

    pub fn queue_text(
        &mut self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        bytes: Vec<u8>,
    ) -> Result<(), SupervisorError> {
        self.ensure_started()?;
        let stream = self.stream_for_connection(connection)?;

        if bytes == b"pong" {
            let known = self
                .streams
                .get(&stream)
                .and_then(|runtime| runtime.tag_for_observed_connection(epoch));
            if known.is_none() || !known.is_some_and(|(_, current)| current) {
                return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
            }
            return self.push_ingress(
                stream,
                Ingress::Pong {
                    stream,
                    epoch,
                    stamp,
                },
            );
        }

        let queue_len = self.queue.len();
        let global_raw_room = queue_len < self.raw_item_limit;
        let (tag, current, attempt, per_stream_room) = {
            let runtime = self
                .streams
                .get_mut(&stream)
                .ok_or(SupervisorError::UnknownConnection(connection))?;
            let (tag, current) = runtime
                .tag_for_observed_connection(epoch)
                .ok_or(SupervisorError::UnknownConnectionEpoch { connection, epoch })?;
            let attempt_value = runtime
                .capture_attempt_frontier
                .checked_add(1)
                .ok_or(SupervisorError::CounterExhausted("CaptureAttemptNo"))?;
            let attempt = CaptureAttemptNo::new(attempt_value)?;
            runtime.capture_attempt_frontier = attempt_value;

            let next_bytes = runtime.queued_raw_bytes.checked_add(bytes.len());
            let per_stream_room = runtime.queued_raw_frames
                < self.queue_policy.max_raw_frames_per_stream
                && next_bytes
                    .is_some_and(|value| value <= self.queue_policy.max_raw_bytes_per_stream)
                && bytes.len() <= self.queue_policy.max_raw_message_bytes;
            (tag, current, attempt, per_stream_room)
        };

        if !current {
            if self.queue.len() >= self.queue_policy.max_total_items {
                self.halted = true;
                return Err(SupervisorError::QueueExhausted { stream });
            }
            self.queue.push_back(Ingress::Raw {
                stream,
                tag,
                attempt,
                stamp,
                bytes,
                counts_toward_bounds: false,
            });
            return Ok(());
        }

        if global_raw_room && per_stream_room {
            let runtime = self
                .streams
                .get_mut(&stream)
                .ok_or(SupervisorError::UnknownConnection(connection))?;
            runtime.queued_raw_frames += 1;
            runtime.queued_raw_bytes = runtime.queued_raw_bytes.checked_add(bytes.len()).ok_or(
                SupervisorError::InvalidConfiguration("queued raw bytes overflow"),
            )?;
            self.queue.push_back(Ingress::Raw {
                stream,
                tag,
                attempt,
                stamp,
                bytes,
                counts_toward_bounds: true,
            });
            return Ok(());
        }

        self.push_ingress(
            stream,
            Ingress::QueueGap {
                stream,
                tag,
                attempt,
                stamp,
            },
        )
    }

    pub fn queue_tick(&mut self, stamp: ReceiveStamp) -> Result<(), SupervisorError> {
        self.ensure_started()?;
        let stream_ids: Vec<StreamId> = self.streams.keys().copied().collect();
        for stream in stream_ids {
            let candidate = {
                let runtime = self
                    .streams
                    .get_mut(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration("missing stream"))?;
                if runtime.transport != Transport::Up {
                    None
                } else if runtime
                    .pong_deadline_ns
                    .is_some_and(|deadline| stamp.monotonic_ns >= deadline)
                    && !runtime.pong_timeout_queued
                {
                    let deadline_ns = runtime.pong_deadline_ns.unwrap_or(stamp.monotonic_ns);
                    let timer_id = next_timer_id(runtime)?;
                    runtime.pong_timeout_queued = true;
                    Some(Ingress::PongTimeout {
                        stream,
                        epoch: runtime.binding.tag.connection,
                        stamp,
                        timer_id,
                        deadline_ns,
                    })
                } else if runtime.pong_deadline_ns.is_none()
                    && runtime
                        .next_ping_due_ns
                        .is_some_and(|deadline| stamp.monotonic_ns >= deadline)
                    && !runtime.ping_timer_queued
                {
                    let deadline_ns = runtime.next_ping_due_ns.unwrap_or(stamp.monotonic_ns);
                    let timer_id = next_timer_id(runtime)?;
                    runtime.ping_timer_queued = true;
                    Some(Ingress::PingTimer {
                        stream,
                        epoch: runtime.binding.tag.connection,
                        stamp,
                        timer_id,
                        deadline_ns,
                    })
                } else {
                    None
                }
            };
            if let Some(candidate) = candidate {
                self.push_ingress(stream, candidate)?;
            }
        }
        Ok(())
    }

    pub fn drain_one(
        &mut self,
        sink: &mut impl RecordSink,
    ) -> Result<Option<DrainResult>, SupervisorError> {
        self.ensure_started()?;
        let Some(ingress) = self.queue.pop_front() else {
            return Ok(None);
        };

        let result = match ingress {
            Ingress::Connected {
                stream,
                epoch,
                stamp,
            } => self.handle_connected(stream, epoch, stamp, sink),
            Ingress::Disconnected {
                stream,
                epoch,
                stamp,
            } => self.handle_disconnected(stream, epoch, stamp, sink),
            Ingress::Raw {
                stream,
                tag,
                attempt,
                stamp,
                bytes,
                counts_toward_bounds,
            } => {
                if counts_toward_bounds {
                    let runtime = self
                        .streams
                        .get_mut(&stream)
                        .ok_or(SupervisorError::InvalidConfiguration("missing stream"))?;
                    runtime.queued_raw_frames = runtime.queued_raw_frames.saturating_sub(1);
                    runtime.queued_raw_bytes = runtime.queued_raw_bytes.saturating_sub(bytes.len());
                }
                self.handle_raw(stream, tag, attempt, stamp, bytes, sink)
            }
            Ingress::QueueGap {
                stream,
                tag,
                attempt,
                stamp,
            } => self.handle_queue_gap(stream, tag, attempt, stamp, sink),
            Ingress::Pong {
                stream,
                epoch,
                stamp,
            } => self.handle_pong(stream, epoch, stamp, sink),
            Ingress::PingTimer {
                stream,
                epoch,
                stamp,
                timer_id,
                deadline_ns,
            } => self.handle_ping_timer(stream, epoch, stamp, timer_id, deadline_ns, sink),
            Ingress::PongTimeout {
                stream,
                epoch,
                stamp,
                timer_id,
                deadline_ns,
            } => self.handle_pong_timeout(stream, epoch, stamp, timer_id, deadline_ns, sink),
        };
        result.map(Some)
    }

    fn handle_connected(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let (connection, symbol, current_epoch) = {
            let runtime =
                self.streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing connected stream",
                    ))?;
            (
                runtime.binding.connection_id,
                runtime
                    .binding
                    .spec
                    .instrument
                    .native_symbol
                    .as_str()
                    .to_owned(),
                runtime.binding.tag.connection,
            )
        };
        if epoch != current_epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }

        let record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Up,
                },
            }),
            sink,
        )?;

        let next_ping_due_ns = stamp
            .monotonic_ns
            .checked_add(HEARTBEAT_INTERVAL_NS)
            .ok_or(SupervisorError::TimeOverflow)?;
        let runtime =
            self.streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing connected stream",
                ))?;
        runtime.transport = Transport::Up;
        runtime.subscription = SubscriptionState::AwaitingAck;
        runtime.reconnect_failures = 0;
        runtime.reconnect_not_before_ns = None;
        runtime.next_ping_due_ns = Some(next_ping_due_ns);
        runtime.pong_deadline_ns = None;
        runtime.ping_timer_queued = false;
        runtime.pong_timeout_queued = false;

        Ok(DrainResult {
            records: vec![record],
            commands: vec![TransportCommand::SendText {
                connection,
                epoch,
                text: subscribe_text(&symbol),
            }],
            events: vec![SupervisorEvent::TransportRecorded {
                stream,
                record,
                value: Transport::Up,
            }],
        })
    }

    fn handle_disconnected(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        self.record_disconnect_and_advance(stream, epoch, stamp, sink)
    }

    fn handle_pong(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let (connection, current_epoch) = {
            let runtime = self
                .streams
                .get(&stream)
                .ok_or(SupervisorError::InvalidConfiguration("missing pong stream"))?;
            (
                runtime.binding.connection_id,
                runtime.binding.tag.connection,
            )
        };
        if epoch != current_epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }

        let record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Up,
                },
            }),
            sink,
        )?;
        let next_ping_due_ns = stamp
            .monotonic_ns
            .checked_add(HEARTBEAT_INTERVAL_NS)
            .ok_or(SupervisorError::TimeOverflow)?;
        let runtime = self
            .streams
            .get_mut(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing pong stream"))?;
        runtime.transport = Transport::Up;
        runtime.pong_deadline_ns = None;
        runtime.next_ping_due_ns = Some(next_ping_due_ns);
        runtime.ping_timer_queued = false;
        runtime.pong_timeout_queued = false;

        Ok(DrainResult {
            records: vec![record],
            commands: Vec::new(),
            events: vec![
                SupervisorEvent::TransportRecorded {
                    stream,
                    record,
                    value: Transport::Up,
                },
                SupervisorEvent::PongRecorded { stream, record },
            ],
        })
    }

    fn handle_ping_timer(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        timer_id: u64,
        deadline_ns: u64,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let (connection, current_epoch) = {
            let runtime =
                self.streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing timer stream",
                    ))?;
            (
                runtime.binding.connection_id,
                runtime.binding.tag.connection,
            )
        };
        if epoch != current_epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }

        let record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::Timer {
                    stream,
                    timer_id,
                    deadline_ns,
                },
            }),
            sink,
        )?;
        let pong_deadline_ns = stamp
            .monotonic_ns
            .checked_add(PONG_TIMEOUT_NS_V1)
            .ok_or(SupervisorError::TimeOverflow)?;
        let runtime =
            self.streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing timer stream",
                ))?;
        runtime.ping_timer_queued = false;
        runtime.next_ping_due_ns = None;
        runtime.pong_deadline_ns = Some(pong_deadline_ns);

        Ok(DrainResult {
            records: vec![record],
            commands: vec![TransportCommand::SendText {
                connection,
                epoch,
                text: "ping".to_owned(),
            }],
            events: vec![SupervisorEvent::HeartbeatTimerRecorded {
                stream,
                record,
                timer_id,
            }],
        })
    }

    fn handle_pong_timeout(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        timer_id: u64,
        deadline_ns: u64,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let timer_record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::Timer {
                    stream,
                    timer_id,
                    deadline_ns,
                },
            }),
            sink,
        )?;
        if let Some(runtime) = self.streams.get_mut(&stream) {
            runtime.pong_timeout_queued = false;
        }
        let mut result = self.record_disconnect_and_advance(stream, epoch, stamp, sink)?;
        result.records.insert(0, timer_record);
        result.events.insert(
            0,
            SupervisorEvent::HeartbeatTimerRecorded {
                stream,
                record: timer_record,
                timer_id,
            },
        );
        Ok(result)
    }

    fn handle_queue_gap(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let current_tag = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing gap stream"))?
            .binding
            .tag;
        if tag != current_tag {
            self.halted = true;
            return Err(SupervisorError::InvalidConfiguration(
                "queue gap crossed an epoch boundary",
            ));
        }
        let record = self.persist_gap(
            stream,
            tag,
            stamp,
            Reason::QueueOverflow,
            Some(GapLoss {
                range: Some((attempt, attempt)),
                loss_count: Some(1),
            }),
            sink,
        )?;
        let runtime = self
            .streams
            .get_mut(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing gap stream"))?;
        runtime.subscription = SubscriptionState::Degraded;
        runtime.continuity.clear_for_new_generation();
        runtime.last_market_record = None;

        Ok(DrainResult {
            records: vec![record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::QueueGapRecorded {
                stream,
                record,
                lost_attempt: attempt,
            }],
        })
    }

    fn handle_raw(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
        bytes: Vec<u8>,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let raw_record = self.persist_record(
            stamp,
            Record::RawInput(RawInput {
                context: stamp.wire_context(self.active_context),
                stream,
                tag,
                attempt,
                bytes: bytes.clone(),
            }),
            sink,
        )?;

        let current_tag = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?
            .binding
            .tag;
        if tag != current_tag {
            return Ok(DrainResult {
                records: vec![raw_record],
                commands: Vec::new(),
                events: vec![SupervisorEvent::ObsoleteRawRecorded {
                    stream,
                    record: raw_record,
                    tag,
                }],
            });
        }

        let symbol = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?
            .binding
            .spec
            .instrument
            .native_symbol
            .as_str()
            .to_owned();
        match classify_inbound(&bytes, &symbol) {
            InboundKind::SubscribeAck => {
                let runtime = self
                    .streams
                    .get_mut(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?;
                if runtime.subscription != SubscriptionState::AwaitingAck {
                    return self.fail_current_stream_after_raw(
                        stream,
                        stamp,
                        raw_record,
                        Reason::Unknown,
                        sink,
                    );
                }
                runtime.subscription = SubscriptionState::AwaitingSnapshot;
                runtime.continuity.clear_for_new_generation();
                Ok(DrainResult {
                    records: vec![raw_record],
                    commands: Vec::new(),
                    events: vec![SupervisorEvent::SubscriptionAccepted {
                        stream,
                        record: raw_record,
                    }],
                })
            }
            InboundKind::SubscribeFailure => {
                let mut result = self.fail_current_stream_after_raw(
                    stream,
                    stamp,
                    raw_record,
                    Reason::Unknown,
                    sink,
                )?;
                result.events.push(SupervisorEvent::SubscriptionFailed {
                    stream,
                    record: raw_record,
                });
                Ok(result)
            }
            InboundKind::Book(frame) => {
                let state = self
                    .streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?
                    .subscription;
                if !matches!(
                    state,
                    SubscriptionState::AwaitingSnapshot | SubscriptionState::CapturingRaw
                ) {
                    return self.fail_current_stream_after_raw(
                        stream,
                        stamp,
                        raw_record,
                        Reason::Unknown,
                        sink,
                    );
                }

                let outcome = {
                    let runtime = self
                        .streams
                        .get_mut(&stream)
                        .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?;
                    runtime.continuity.observe(&frame)
                };
                let mut result = DrainResult {
                    records: vec![raw_record],
                    commands: Vec::new(),
                    events: vec![SupervisorEvent::RawBookRecorded {
                        stream,
                        record: raw_record,
                        attempt,
                        continuity: outcome,
                    }],
                };
                match outcome {
                    ContinuityOutcome::AnchorCandidate { .. }
                    | ContinuityOutcome::Continuous { .. }
                    | ContinuityOutcome::DuplicateDiagnostic { .. } => {
                        let runtime = self
                            .streams
                            .get_mut(&stream)
                            .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?;
                        runtime.subscription = SubscriptionState::CapturingRaw;
                        runtime.last_market_record = Some(raw_record);
                    }
                    ContinuityOutcome::Gap { .. }
                    | ContinuityOutcome::ResetOrDiscontinuity { .. }
                    | ContinuityOutcome::SnapshotIntervalMismatch { .. }
                    | ContinuityOutcome::NeedsSnapshot { .. }
                    | ContinuityOutcome::UnexpectedSnapshot { .. } => {
                        let gap_record = self.persist_gap(
                            stream,
                            current_tag,
                            stamp,
                            Reason::SourceGap,
                            None,
                            sink,
                        )?;
                        let runtime = self
                            .streams
                            .get_mut(&stream)
                            .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?;
                        runtime.subscription = SubscriptionState::Degraded;
                        runtime.last_market_record = None;
                        result.records.push(gap_record);
                        result.events.push(SupervisorEvent::SourceGapRecorded {
                            stream,
                            record: gap_record,
                            reason: Reason::SourceGap,
                        });
                    }
                }
                Ok(result)
            }
            InboundKind::Rejected => self.fail_current_stream_after_raw(
                stream,
                stamp,
                raw_record,
                Reason::DecodeRejected,
                sink,
            ),
        }
    }

    fn fail_current_stream_after_raw(
        &mut self,
        stream: StreamId,
        stamp: ReceiveStamp,
        raw_record: RecordNo,
        reason: Reason,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let tag = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration(
                "missing failed stream",
            ))?
            .binding
            .tag;
        let gap_record = self.persist_gap(stream, tag, stamp, reason, None, sink)?;
        let runtime =
            self.streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing failed stream",
                ))?;
        runtime.subscription = SubscriptionState::Degraded;
        runtime.continuity.clear_for_new_generation();
        runtime.last_market_record = None;
        Ok(DrainResult {
            records: vec![raw_record, gap_record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::SourceGapRecorded {
                stream,
                record: gap_record,
                reason,
            }],
        })
    }

    fn record_disconnect_and_advance(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<DrainResult, SupervisorError> {
        let (connection, old_tag, book_id) = {
            let runtime =
                self.streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing disconnected stream",
                    ))?;
            (
                runtime.binding.connection_id,
                runtime.binding.tag,
                runtime
                    .binding
                    .book_id
                    .ok_or(SupervisorError::InvalidConfiguration("missing book id"))?,
            )
        };
        if old_tag.connection != epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }

        let down_record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Down,
                },
            }),
            sink,
        )?;

        let next_connection = old_tag.connection.checked_next()?;
        let next_subscription = old_tag.subscription.checked_next()?;
        let old_book = old_tag
            .book
            .ok_or(SupervisorError::InvalidConfiguration("missing book epoch"))?;
        let next_book = old_book.checked_next()?;

        let connection_record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Connection {
                        owner: connection,
                        expected: old_tag.connection,
                        next: next_connection,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;
        if let Some(runtime) = self.streams.get_mut(&stream) {
            runtime.binding.tag.connection = next_connection;
        }

        let subscription_record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Subscription {
                        owner: stream,
                        expected: old_tag.subscription,
                        next: next_subscription,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;
        if let Some(runtime) = self.streams.get_mut(&stream) {
            runtime.binding.tag.subscription = next_subscription;
        }

        let book_record = self.persist_record(
            stamp,
            Record::Control(ControlRecord {
                context: stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Book {
                        owner: book_id,
                        expected: old_book,
                        next: next_book,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;

        let (new_tag, delay_ns) = {
            let runtime =
                self.streams
                    .get_mut(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing disconnected stream",
                    ))?;
            runtime.previous_tag = Some(old_tag);
            runtime.binding.tag.book = Some(next_book);
            runtime.transport = Transport::Unknown;
            runtime.subscription = SubscriptionState::Backoff;
            runtime.continuity.clear_for_new_generation();
            runtime.next_ping_due_ns = None;
            runtime.pong_deadline_ns = None;
            runtime.ping_timer_queued = false;
            runtime.pong_timeout_queued = false;
            runtime.last_market_record = None;
            runtime.reconnect_failures = runtime.reconnect_failures.saturating_add(1);
            let delay_ns = reconnect_delay_ns(runtime.reconnect_failures, stream);
            runtime.reconnect_not_before_ns = Some(
                stamp
                    .monotonic_ns
                    .checked_add(delay_ns)
                    .ok_or(SupervisorError::TimeOverflow)?,
            );
            (runtime.binding.tag, delay_ns)
        };

        Ok(DrainResult {
            records: vec![
                down_record,
                connection_record,
                subscription_record,
                book_record,
            ],
            commands: vec![
                TransportCommand::Close { connection, epoch },
                TransportCommand::ReconnectAfter {
                    connection,
                    epoch: new_tag.connection,
                    delay_ns,
                },
            ],
            events: vec![
                SupervisorEvent::TransportRecorded {
                    stream,
                    record: down_record,
                    value: Transport::Down,
                },
                SupervisorEvent::EpochAdvanced {
                    stream,
                    records: vec![connection_record, subscription_record, book_record],
                    tag: new_tag,
                },
            ],
        })
    }

    fn persist_gap(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        stamp: ReceiveStamp,
        reason: Reason,
        loss: Option<GapLoss>,
        sink: &mut impl RecordSink,
    ) -> Result<RecordNo, SupervisorError> {
        self.persist_record(
            stamp,
            Record::Gap(Gap {
                context: stamp.wire_context(self.active_context),
                scope: GapScope::ExplicitTargets(vec![GapTarget {
                    stream,
                    tag,
                    range: loss.and_then(|loss| loss.range),
                    loss_count: loss.and_then(|loss| loss.loss_count),
                }]),
                reason,
            }),
            sink,
        )
    }

    fn persist_record(
        &mut self,
        _stamp: ReceiveStamp,
        value: Record,
        sink: &mut impl RecordSink,
    ) -> Result<RecordNo, SupervisorError> {
        let record_no = self
            .next_record_no
            .ok_or(SupervisorError::CounterExhausted("RecordNo"))?;
        let frame = RecordFrame {
            record_no,
            segment_no: self.segment_no,
            value,
        };
        let receipt = match sink.persist(&frame, self.recording_gate) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.halted = true;
                return Err(SupervisorError::Persistence(error));
            }
        };
        if receipt.through != record_no {
            self.halted = true;
            return Err(SupervisorError::PersistenceReceiptMismatch {
                expected: record_no,
                actual: receipt.through,
            });
        }
        if !receipt.achieved.covers(self.recording_gate) {
            self.halted = true;
            return Err(SupervisorError::PersistenceGateTooWeak {
                required: self.recording_gate,
                achieved: receipt.achieved,
            });
        }
        self.next_record_no = record_no.checked_next().ok();
        Ok(record_no)
    }

    fn stream_for_connection(&self, connection: ConnectionId) -> Result<StreamId, SupervisorError> {
        self.by_connection
            .get(&connection)
            .copied()
            .ok_or(SupervisorError::UnknownConnection(connection))
    }

    fn push_ingress(&mut self, stream: StreamId, ingress: Ingress) -> Result<(), SupervisorError> {
        if self.queue.len() >= self.queue_policy.max_total_items {
            self.halted = true;
            return Err(SupervisorError::QueueExhausted { stream });
        }
        self.queue.push_back(ingress);
        Ok(())
    }

    fn ensure_running(&self) -> Result<(), SupervisorError> {
        if self.halted {
            Err(SupervisorError::Halted)
        } else {
            Ok(())
        }
    }

    fn ensure_started(&self) -> Result<(), SupervisorError> {
        self.ensure_running()?;
        if self.started {
            Ok(())
        } else {
            Err(SupervisorError::NotStarted)
        }
    }
}

fn next_timer_id(runtime: &mut StreamRuntime) -> Result<u64, SupervisorError> {
    let next = runtime
        .timer_frontier
        .checked_add(1)
        .ok_or(SupervisorError::CounterExhausted("TimerId"))?;
    runtime.timer_frontier = next;
    Ok(next)
}

pub fn reconnect_delay_ns(attempt: u32, stream: StreamId) -> u64 {
    let shift = attempt.saturating_sub(1).min(4);
    let scaled = RECONNECT_BASE_NS_V1
        .checked_shl(shift)
        .unwrap_or(RECONNECT_MAX_NS_V1)
        .min(RECONNECT_MAX_NS_V1);
    let room = RECONNECT_MAX_NS_V1.saturating_sub(scaled);
    let jitter_span = (scaled / 4).min(room);
    if jitter_span == 0 {
        return scaled;
    }
    let seed = u64::from(attempt) + u64::from(stream.get());
    scaled + (seed % (jitter_span + 1))
}

fn subscribe_text(symbol: &str) -> String {
    format!(
        r#"{{"op":"subscribe","args":[{{"instType":"usdt-futures","topic":"books50","symbol":"{symbol}"}}]}}"#
    )
}

enum InboundKind {
    SubscribeAck,
    SubscribeFailure,
    Book(crate::Books50Frame),
    Rejected,
}

fn classify_inbound(bytes: &[u8], expected_symbol: &str) -> InboundKind {
    match decode_message_with_limits(bytes, DecodeLimits::default()) {
        Ok(BitgetMessage::Books50(frame))
            if frame.category == Category::UsdtFutures
                && frame.topic == Topic::Books50
                && frame.symbol == expected_symbol =>
        {
            return InboundKind::Book(frame);
        }
        Ok(BitgetMessage::Books50(_)) | Ok(BitgetMessage::PublicTrade(_)) => {
            return InboundKind::Rejected;
        }
        Err(_) => {}
    }

    let limits = ParserLimits {
        max_nesting_depth: 16,
        max_container_items: 256,
        max_string_bytes: 4096,
    };
    let Ok(JsonValue::Object(root)) = parse_json(bytes, limits) else {
        return InboundKind::Rejected;
    };
    let Some(event) = json_string(json_field(&root, "event")) else {
        return InboundKind::Rejected;
    };

    if event == "error" {
        return InboundKind::SubscribeFailure;
    }
    if event != "subscribe" {
        return InboundKind::Rejected;
    }
    let Some(JsonValue::Object(arg)) = json_field(&root, "arg") else {
        return InboundKind::Rejected;
    };
    if json_string(json_field(arg, "instType")) != Some("usdt-futures")
        || json_string(json_field(arg, "topic")) != Some("books50")
        || json_string(json_field(arg, "symbol")) != Some(expected_symbol)
    {
        return InboundKind::Rejected;
    }

    match json_scalar_text(json_field(&root, "code")) {
        None | Some("0") => InboundKind::SubscribeAck,
        Some(_) => InboundKind::SubscribeFailure,
    }
}

fn json_field<'a>(object: &'a [(String, JsonValue)], name: &str) -> Option<&'a JsonValue> {
    object
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value))
}

fn json_string(value: Option<&JsonValue>) -> Option<&str> {
    match value {
        Some(JsonValue::String(text)) => Some(text),
        _ => None,
    }
}

fn json_scalar_text(value: Option<&JsonValue>) -> Option<&str> {
    match value {
        Some(JsonValue::String(text) | JsonValue::Number(text)) => Some(text),
        _ => None,
    }
}
