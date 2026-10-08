use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

use domain::capture_session as session;
pub use domain::capture_session::{HeartbeatPolicy, PersistError, PersistenceReceipt};
use domain::event::{ActiveContext, InputContext};
use domain::identity::{
    BookEpoch, BookId, CaptureAttemptNo, Channel, ConnectionEpoch, ConnectionId, EpochTag,
    IdentityError, LocalUnixNs, MarketKind, MonotonicNs, RecordNo, SegmentNo, StreamBinding,
    StreamId, SubscriptionEpoch,
};
use domain::policy::{RecordingGate, WatermarkKind};
use domain::record::{
    Control, ControlRecord, EpochChange, Gap, GapScope, GapTarget, RawInput, Reason, Record,
    RecordFrame, Transport, WireContext,
};

use crate::json::{JsonValue, ParserLimits, parse_json};
use crate::{
    BitgetMessage, Category, ContinuityClassifier, ContinuityOutcome, DecodeLimits, Topic,
    decode_message_with_limits,
};

/// Sorted fixed registry with observable backing capacity and no growth.
struct FixedRegistry<K, V> {
    entries: Vec<(K, V)>,
}
impl<K: Ord, V> FixedRegistry<K, V> {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(MAX_CONFIGURED_STREAMS),
        }
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
    fn capacity(&self) -> usize {
        self.entries.capacity()
    }
    fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .iter()
            .find(|(stored, _)| stored == key)
            .map(|(_, value)| value)
    }
    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find(|(stored, _)| stored == key)
            .map(|(_, value)| value)
    }
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(prior) = self.get_mut(&key) {
            return Some(std::mem::replace(prior, value));
        }
        assert!(
            self.entries.len() < MAX_CONFIGURED_STREAMS,
            "fixed registry capacity"
        );
        self.entries.push((key, value));
        self.entries
            .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        None
    }
    fn remove(&mut self, key: &K) -> Option<V> {
        self.entries
            .iter()
            .position(|(stored, _)| stored == key)
            .map(|index| self.entries.remove(index).1)
    }
    fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.iter().map(|(key, _)| key)
    }
    fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().map(|(_, value)| value)
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().map(|(_, value)| value)
    }
    fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.entries.iter_mut().map(|(key, value)| (&*key, value))
    }
}
impl<K: Ord, V> std::ops::Index<&K> for FixedRegistry<K, V> {
    type Output = V;
    fn index(&self, key: &K) -> &Self::Output {
        self.get(key).expect("configured registry key")
    }
}

pub const BITGET_PUBLIC_WS_ENDPOINT: &str = "wss://ws.bitget.com/v3/ws/public";
pub const SUPERVISOR_POLICY_VERSION: u32 = HeartbeatPolicy::SupervisorV2.revision();
pub const MAX_CONFIGURED_STREAMS: usize = 4;
pub const HEARTBEAT_INTERVAL_NS: u64 = HeartbeatPolicy::SupervisorV2.ping_interval_ns();
pub const PONG_TIMEOUT_NS_V1: u64 = HeartbeatPolicy::SupervisorV2.pong_timeout_ns();
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

trait RecordSink {
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
    Authority(session::AuthorityError),
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
    pub accounted_attempt_frontier: u64,
    pub capture_terminated: bool,
    pub session_disposition: Option<session::SessionDisposition>,
    pub first_failure: Option<session::TerminalFailure>,
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
    ObsoleteControl {
        stream: StreamId,
        epoch: ConnectionEpoch,
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
struct CoreDrainResult {
    pub records: Vec<RecordNo>,
    pub commands: Vec<TransportCommand>,
    pub events: Vec<SupervisorEvent>,
}

#[derive(Clone, Copy)]
struct PendingDisconnect {
    epoch: ConnectionEpoch,
    stamp: ReceiveStamp,
    book_id: BookId,
    next_connection: ConnectionEpoch,
    next_subscription: SubscriptionEpoch,
    next_book: BookEpoch,
    reconnect_attempt: u32,
    delay_ns: u64,
    reconnect_not_before_ns: u64,
}

struct StreamRuntime {
    binding: StreamBinding,
    previous_tag: Option<EpochTag>,
    transport: Transport,
    subscription: SubscriptionState,
    continuity: ContinuityClassifier,
    capture_attempt_frontier: u64,
    accounted_attempt_frontier: u64,
    capture_terminated: bool,
    queued_raw_frames: usize,
    queued_raw_bytes: usize,
    reconnect_failures: u32,
    reconnect_not_before_ns: Option<u64>,
    last_market_record: Option<RecordNo>,
    pending_disconnect: Option<PendingDisconnect>,
    close_settled: bool,
    terminal_down_epoch: Option<ConnectionEpoch>,
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
            accounted_attempt_frontier: self.accounted_attempt_frontier,
            capture_terminated: self.capture_terminated,
            session_disposition: None,
            first_failure: None,
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

#[derive(Clone)]
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
    },
    RejectedStaleRaw {
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
    },
    QueueGap {
        stream: StreamId,
        tag: EpochTag,
        first_attempt: CaptureAttemptNo,
        last_attempt: CaptureAttemptNo,
        loss_count: u64,
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

impl Ingress {
    fn belongs_to_connection_epoch(&self, stream: StreamId, epoch: ConnectionEpoch) -> bool {
        match self {
            Self::Connected {
                stream: observed,
                epoch: observed_epoch,
                ..
            }
            | Self::Disconnected {
                stream: observed,
                epoch: observed_epoch,
                ..
            }
            | Self::Pong {
                stream: observed,
                epoch: observed_epoch,
                ..
            }
            | Self::PingTimer {
                stream: observed,
                epoch: observed_epoch,
                ..
            }
            | Self::PongTimeout {
                stream: observed,
                epoch: observed_epoch,
                ..
            } => *observed == stream && *observed_epoch == epoch,
            Self::Raw {
                stream: observed,
                tag,
                ..
            }
            | Self::RejectedStaleRaw {
                stream: observed,
                tag,
                ..
            }
            | Self::QueueGap {
                stream: observed,
                tag,
                ..
            } => *observed == stream && tag.connection == epoch,
        }
    }
}

#[derive(Clone, Copy)]
struct GapLoss {
    range: Option<(CaptureAttemptNo, CaptureAttemptNo)>,
    loss_count: Option<u64>,
}

struct SupervisorCore {
    active_context: ActiveContext,
    recording_gate: RecordingGate,
    segment_no: SegmentNo,
    next_record_no: Option<RecordNo>,
    queue_policy: QueuePolicy,
    raw_item_limit: usize,
    queued_raw_items: usize,
    work_limit: usize,
    external_work: usize,
    cut_remaining: Option<usize>,
    marker_settled: bool,
    streams: FixedRegistry<StreamId, StreamRuntime>,
    by_connection: FixedRegistry<ConnectionId, StreamId>,
    queue: VecDeque<Ingress>,
    started: bool,
    halted: bool,
    // The bound public path derives heartbeat entitlement at the authority's
    // exact receipt boundary. Local protocol fields are only mirrors there.
    authority_heartbeat: bool,
    allow_generated_disconnect: bool,
}

impl SupervisorCore {
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
        let mut registered = Vec::with_capacity(config.streams.len());
        let mut streams = FixedRegistry::new();
        let mut by_connection = FixedRegistry::new();

        for binding in config.streams {
            binding.validate_registration(&registered)?;
            if binding.channel != Channel::BookNormal
                || binding.spec.instrument.venue.as_str() != "bitget"
                || binding.spec.instrument.product_namespace.as_str() != "usdt-futures"
                || !matches!(
                    binding.spec.instrument.market,
                    MarketKind::Perpetual | MarketKind::DatedFuture
                )
                || binding.book_id.is_none()
                || binding.tag.book.is_none()
            {
                return Err(SupervisorError::InvalidConfiguration(
                    "regular bitget usdt-futures books50 binding",
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
            registered.push(binding.clone());
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
                    accounted_attempt_frontier: 0,
                    capture_terminated: false,
                    queued_raw_frames: 0,
                    queued_raw_bytes: 0,
                    reconnect_failures: 0,
                    reconnect_not_before_ns: None,
                    last_market_record: None,
                    pending_disconnect: None,
                    close_settled: false,
                    terminal_down_epoch: None,
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
            queued_raw_items: 0,
            work_limit: queue_policy.max_total_items - streams.len() - 1,
            external_work: 0,
            cut_remaining: None,
            marker_settled: false,
            streams,
            by_connection,
            queue: VecDeque::with_capacity(queue_policy.max_total_items - stream_ids.len() - 1),
            started: false,
            halted: false,
            authority_heartbeat: false,
            allow_generated_disconnect: true,
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
        bytes: impl AsRef<[u8]>,
    ) -> Result<(), SupervisorError> {
        let bytes = bytes.as_ref();
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

        let (tag, current, attempt, per_stream_room) = {
            let runtime = self
                .streams
                .get(&stream)
                .ok_or(SupervisorError::UnknownConnection(connection))?;
            let (tag, current) = runtime
                .tag_for_observed_connection(epoch)
                .ok_or(SupervisorError::UnknownConnectionEpoch { connection, epoch })?;
            let Some(attempt_value) = runtime.capture_attempt_frontier.checked_add(1) else {
                return Err(SupervisorError::CounterExhausted("CaptureAttemptNo"));
            };
            let attempt = CaptureAttemptNo::new(attempt_value)?;
            let next_bytes = runtime.queued_raw_bytes.checked_add(bytes.len());
            let per_stream_room = runtime.queued_raw_frames
                < self.queue_policy.max_raw_frames_per_stream
                && next_bytes
                    .is_some_and(|value| value <= self.queue_policy.max_raw_bytes_per_stream)
                && bytes.len() <= self.queue_policy.max_raw_message_bytes;
            (tag, current, attempt, per_stream_room)
        };

        let global_raw_room = self.queued_raw_items < self.raw_item_limit
            && self.queue.len() + self.external_work < self.work_limit;

        if global_raw_room && per_stream_room {
            let runtime = self
                .streams
                .get_mut(&stream)
                .ok_or(SupervisorError::UnknownConnection(connection))?;
            runtime.queued_raw_frames += 1;
            runtime.queued_raw_bytes = runtime.queued_raw_bytes.checked_add(bytes.len()).ok_or(
                SupervisorError::InvalidConfiguration("queued raw bytes overflow"),
            )?;
            runtime.capture_attempt_frontier = attempt.get();
            self.queued_raw_items += 1;
            self.queue.push_back(Ingress::Raw {
                stream,
                tag,
                attempt,
                stamp,
                bytes: bytes.to_vec(),
            });
            return Ok(());
        }

        if current {
            self.queue_gap_loss(stream, tag, attempt, stamp)?;
        } else {
            self.push_loss_ingress(
                stream,
                Ingress::RejectedStaleRaw {
                    stream,
                    tag,
                    attempt,
                    stamp,
                },
            )?;
        }

        self.streams
            .get_mut(&stream)
            .ok_or(SupervisorError::UnknownConnection(connection))?
            .capture_attempt_frontier = attempt.get();
        Ok(())
    }

    /// Returns one ingress outcome or one ready disconnect completion.
    /// A durable terminal Down and its Close are returned before any fallible
    /// epoch completion. Call again, even with empty ingress, to finish pending
    /// transitions; completion errors halt without retracting earlier commands.
    pub fn drain_one(
        &mut self,
        sink: &mut impl RecordSink,
    ) -> Result<Option<CoreDrainResult>, SupervisorError> {
        self.ensure_started()?;

        let mut ready = CoreDrainResult::default();
        if self.cut_remaining.is_none() || self.marker_settled {
            self.finish_ready_disconnects(sink, &mut ready)?;
        }
        if !ready.records.is_empty() || !ready.commands.is_empty() || !ready.events.is_empty() {
            return Ok(Some(ready));
        }

        let Some(ingress) = self.queue.pop_front() else {
            return Ok(None);
        };
        if let Some(remaining) = self.cut_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }
        let result = match ingress {
            Ingress::Connected {
                stream,
                epoch,
                stamp,
            } => self.handle_connected(stream, epoch, stamp, sink)?,
            Ingress::Disconnected {
                stream,
                epoch,
                stamp,
            } => self.handle_disconnected(stream, epoch, stamp, sink)?,
            Ingress::Raw {
                stream,
                tag,
                attempt,
                stamp,
                bytes,
            } => {
                let runtime = self
                    .streams
                    .get_mut(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration("missing stream"))?;
                runtime.queued_raw_frames = runtime.queued_raw_frames.saturating_sub(1);
                runtime.queued_raw_bytes = runtime.queued_raw_bytes.saturating_sub(bytes.len());
                self.queued_raw_items = self.queued_raw_items.saturating_sub(1);
                self.handle_raw(stream, tag, attempt, stamp, bytes, sink)?
            }
            Ingress::RejectedStaleRaw {
                stream,
                tag,
                attempt,
                stamp,
            } => self.handle_rejected_stale_raw(stream, tag, attempt, stamp, sink)?,
            Ingress::QueueGap {
                stream,
                tag,
                first_attempt,
                last_attempt,
                loss_count,
                stamp,
            } => self.handle_queue_gap(
                stream,
                tag,
                GapLoss {
                    range: Some((first_attempt, last_attempt)),
                    loss_count: Some(loss_count),
                },
                stamp,
                sink,
            )?,
            Ingress::Pong {
                stream,
                epoch,
                stamp,
            } => self.handle_pong(stream, epoch, stamp, sink)?,
            Ingress::PingTimer { .. } | Ingress::PongTimeout { .. } => {
                return Err(SupervisorError::Authority(
                    session::AuthorityError::TimerAuthorityRequired,
                ));
            }
        };
        Ok(Some(result))
    }

    fn handle_connected(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let (connection, symbol, current_epoch, terminal) = {
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
                runtime.terminal_down_epoch == Some(epoch),
            )
        };
        if epoch != current_epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }
        if terminal {
            return Ok(CoreDrainResult {
                records: Vec::new(),
                commands: Vec::new(),
                events: vec![SupervisorEvent::ObsoleteControl { stream, epoch }],
            });
        }

        if self.streams[&stream].capture_terminated {
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
            return Ok(CoreDrainResult {
                records: vec![record],
                commands: Vec::new(),
                events: vec![SupervisorEvent::TransportRecorded {
                    stream,
                    record,
                    value: Transport::Up,
                }],
            });
        }

        let _next_ping_due_ns = if self.authority_heartbeat {
            None
        } else {
            Some(self.checked_time_add_or_halt(stamp.monotonic_ns, HEARTBEAT_INTERVAL_NS)?)
        };

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

        Ok(CoreDrainResult {
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
    ) -> Result<CoreDrainResult, SupervisorError> {
        self.begin_disconnect(stream, epoch, stamp, sink)
    }

    fn handle_pong(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let (connection, current_epoch, terminal) = {
            let runtime = self
                .streams
                .get(&stream)
                .ok_or(SupervisorError::InvalidConfiguration("missing pong stream"))?;
            (
                runtime.binding.connection_id,
                runtime.binding.tag.connection,
                runtime.terminal_down_epoch == Some(epoch),
            )
        };
        if epoch != current_epoch {
            return Err(SupervisorError::UnknownConnectionEpoch { connection, epoch });
        }
        if terminal {
            return Ok(CoreDrainResult {
                records: Vec::new(),
                commands: Vec::new(),
                events: vec![SupervisorEvent::ObsoleteControl { stream, epoch }],
            });
        }

        if self.streams[&stream].capture_terminated {
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
            return Ok(CoreDrainResult {
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
            });
        }

        let _next_ping_due_ns = if self.authority_heartbeat {
            None
        } else {
            Some(self.checked_time_add_or_halt(stamp.monotonic_ns, HEARTBEAT_INTERVAL_NS)?)
        };

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
        let runtime = self
            .streams
            .get_mut(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing pong stream"))?;
        runtime.transport = Transport::Up;

        Ok(CoreDrainResult {
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

    // A queued timer remains a truthful recorded observation after cancellation.
    // Only its matching private owner may apply heartbeat effects; obsolete
    // observations neither preflight effects nor clear a newer schedule's owner.
    fn persist_timer_observation(
        &mut self,
        stream: StreamId,
        stamp: ReceiveStamp,
        timer_id: u64,
        deadline_ns: u64,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
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
        Ok(CoreDrainResult {
            records: vec![record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::HeartbeatTimerRecorded {
                stream,
                record,
                timer_id,
            }],
        })
    }

    fn handle_queue_gap(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        loss: GapLoss,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let first_attempt =
            loss.range
                .map(|(first, _)| first)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "queue gap missing attempt range",
                ))?;
        let record =
            self.persist_gap(stream, tag, stamp, Reason::QueueOverflow, Some(loss), sink)?;

        let current_tag = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing gap stream"))?
            .binding
            .tag;
        if tag == current_tag {
            let runtime = self
                .streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration("missing gap stream"))?;
            runtime.subscription = SubscriptionState::Degraded;
            runtime.continuity.clear_for_new_generation();
            runtime.last_market_record = None;
        }

        Ok(CoreDrainResult {
            records: vec![record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::QueueGapRecorded {
                stream,
                record,
                lost_attempt: first_attempt,
            }],
        })
    }

    fn handle_rejected_stale_raw(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let raw_record = self.persist_record(
            stamp,
            Record::RawInput(RawInput {
                context: stamp.wire_context(self.active_context),
                stream,
                tag,
                attempt,
                bytes: Vec::new(),
            }),
            sink,
        )?;
        let gap_record = self.persist_gap(stream, tag, stamp, Reason::Unknown, None, sink)?;

        Ok(CoreDrainResult {
            records: vec![raw_record, gap_record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::ObsoleteRawRecorded {
                stream,
                record: raw_record,
                tag,
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
    ) -> Result<CoreDrainResult, SupervisorError> {
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
        if tag != current_tag || self.streams[&stream].capture_terminated {
            return Ok(CoreDrainResult {
                records: vec![raw_record],
                commands: Vec::new(),
                events: vec![SupervisorEvent::ObsoleteRawRecorded {
                    stream,
                    record: raw_record,
                    tag,
                }],
            });
        }
        let terminal = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration("missing raw stream"))?
            .pending_disconnect
            .is_some_and(|pending| pending.epoch == tag.connection);
        if terminal {
            return Ok(CoreDrainResult {
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
                Ok(CoreDrainResult {
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
                let mut result = CoreDrainResult {
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
    ) -> Result<CoreDrainResult, SupervisorError> {
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
        Ok(CoreDrainResult {
            records: vec![raw_record, gap_record],
            commands: Vec::new(),
            events: vec![SupervisorEvent::SourceGapRecorded {
                stream,
                record: gap_record,
                reason,
            }],
        })
    }

    fn begin_disconnect(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let plan = if self.authority_heartbeat && !self.allow_generated_disconnect {
            None
        } else {
            self.preflight_disconnect(stream, epoch, stamp, 0, false)?
        };
        self.persist_disconnect_observation(stream, epoch, stamp, plan, sink)
    }

    fn preflight_disconnect(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        records_before_down: u64,
        down_already_confirmed: bool,
    ) -> Result<Option<PendingDisconnect>, SupervisorError> {
        let (connection, old_tag, book_id, reconnect_failures, duplicate_pending) = {
            let runtime =
                self.streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing disconnected stream",
                    ))?;
            if runtime.binding.tag.connection != epoch {
                return Err(SupervisorError::UnknownConnectionEpoch {
                    connection: runtime.binding.connection_id,
                    epoch,
                });
            }
            (
                runtime.binding.connection_id,
                runtime.binding.tag,
                runtime
                    .binding
                    .book_id
                    .ok_or(SupervisorError::InvalidConfiguration("missing book id"))?,
                runtime.reconnect_failures,
                runtime.terminal_down_epoch == Some(epoch) && !down_already_confirmed,
            )
        };

        if self.streams[&stream].capture_terminated {
            self.ensure_record_capacity(records_before_down + u64::from(!down_already_confirmed))?;
            return Ok(None);
        }
        let required_records = records_before_down
            + if down_already_confirmed {
                3
            } else if duplicate_pending {
                1
            } else {
                4
            };
        self.ensure_record_capacity(required_records)?;
        if duplicate_pending {
            return Ok(None);
        }

        let next_connection = match old_tag.connection.checked_next() {
            Ok(value) => value,
            Err(error) => return self.halt_with(SupervisorError::Identity(error)),
        };
        let next_subscription = match old_tag.subscription.checked_next() {
            Ok(value) => value,
            Err(error) => return self.halt_with(SupervisorError::Identity(error)),
        };
        let old_book = old_tag
            .book
            .ok_or(SupervisorError::InvalidConfiguration("missing book epoch"))?;
        let next_book = match old_book.checked_next() {
            Ok(value) => value,
            Err(error) => return self.halt_with(SupervisorError::Identity(error)),
        };
        let reconnect_attempt = match reconnect_failures.checked_add(1) {
            Some(value) => value,
            None => return self.halt_with(SupervisorError::CounterExhausted("ReconnectAttempt")),
        };
        let delay_ns = reconnect_delay_ns(reconnect_attempt, stream);
        let reconnect_not_before_ns =
            self.checked_time_add_or_halt(stamp.monotonic_ns, delay_ns)?;

        let _ = connection;
        Ok(Some(PendingDisconnect {
            epoch,
            stamp,
            book_id,
            next_connection,
            next_subscription,
            next_book,
            reconnect_attempt,
            delay_ns,
            reconnect_not_before_ns,
        }))
    }

    fn persist_disconnect_observation(
        &mut self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        plan: Option<PendingDisconnect>,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let connection = self
            .streams
            .get(&stream)
            .ok_or(SupervisorError::InvalidConfiguration(
                "missing disconnected stream",
            ))?
            .binding
            .connection_id;

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

        let runtime = self.streams.get_mut(&stream).expect("recorded Down scope");
        runtime.terminal_down_epoch = Some(epoch);
        runtime.transport = Transport::Down;
        runtime.subscription = SubscriptionState::Degraded;
        runtime.continuity.clear_for_new_generation();
        runtime.last_market_record = None;
        let Some(plan) = plan else {
            return Ok(CoreDrainResult {
                records: vec![down_record],
                commands: if self.authority_heartbeat {
                    vec![TransportCommand::Close { connection, epoch }]
                } else {
                    Vec::new()
                },
                events: vec![SupervisorEvent::TransportRecorded {
                    stream,
                    record: down_record,
                    value: Transport::Down,
                }],
            });
        };

        self.install_disconnect_plan(stream, plan)?;

        Ok(CoreDrainResult {
            records: vec![down_record],
            commands: vec![TransportCommand::Close { connection, epoch }],
            events: vec![SupervisorEvent::TransportRecorded {
                stream,
                record: down_record,
                value: Transport::Down,
            }],
        })
    }

    fn install_disconnect_plan(
        &mut self,
        stream: StreamId,
        plan: PendingDisconnect,
    ) -> Result<(), SupervisorError> {
        let runtime =
            self.streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing disconnected stream",
                ))?;
        runtime.transport = Transport::Down;
        runtime.subscription = SubscriptionState::Degraded;
        runtime.continuity.clear_for_new_generation();
        runtime.last_market_record = None;
        runtime.pending_disconnect = Some(plan);
        runtime.close_settled = false;

        Ok(())
    }

    fn disconnect_ready(&self, stream: StreamId) -> bool {
        let Some(runtime) = self.streams.get(&stream) else {
            return false;
        };
        let Some(pending) = runtime.pending_disconnect else {
            return false;
        };
        runtime.close_settled
            && !runtime.capture_terminated
            && !self
                .queue
                .iter()
                .any(|ingress| ingress.belongs_to_connection_epoch(stream, pending.epoch))
    }

    fn finish_ready_disconnect_for(
        &mut self,
        stream: StreamId,
        sink: &mut impl RecordSink,
        result: &mut CoreDrainResult,
    ) -> Result<(), SupervisorError> {
        if !self.disconnect_ready(stream) {
            return Ok(());
        }
        let completed = self.finish_disconnect(stream, sink)?;
        result.records.extend(completed.records);
        result.commands.extend(completed.commands);
        result.events.extend(completed.events);
        Ok(())
    }

    fn finish_ready_disconnects(
        &mut self,
        sink: &mut impl RecordSink,
        result: &mut CoreDrainResult,
    ) -> Result<(), SupervisorError> {
        let ready = self
            .streams
            .keys()
            .copied()
            .find(|stream| self.disconnect_ready(*stream));
        if let Some(stream) = ready {
            self.finish_ready_disconnect_for(stream, sink, result)?;
        }
        Ok(())
    }

    fn finish_disconnect(
        &mut self,
        stream: StreamId,
        sink: &mut impl RecordSink,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let (connection, old_tag, pending) = {
            let runtime =
                self.streams
                    .get(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing disconnected stream",
                    ))?;
            let pending =
                runtime
                    .pending_disconnect
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing pending disconnect",
                    ))?;
            (runtime.binding.connection_id, runtime.binding.tag, pending)
        };
        let Some(old_book) = old_tag.book else {
            return self.halt_with(SupervisorError::InvalidConfiguration("missing book epoch"));
        };
        if old_tag.connection != pending.epoch
            || old_tag.subscription.checked_next().ok() != Some(pending.next_subscription)
            || old_book.checked_next().ok() != Some(pending.next_book)
            || old_tag.connection.checked_next().ok() != Some(pending.next_connection)
        {
            return self.halt_with(SupervisorError::InvalidConfiguration(
                "pending disconnect plan mismatch",
            ));
        }

        self.ensure_record_capacity(3)?;

        let connection_record = self.persist_record(
            pending.stamp,
            Record::Control(ControlRecord {
                context: pending.stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Connection {
                        owner: connection,
                        expected: old_tag.connection,
                        next: pending.next_connection,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;

        let subscription_record = self.persist_record(
            pending.stamp,
            Record::Control(ControlRecord {
                context: pending.stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Subscription {
                        owner: stream,
                        expected: old_tag.subscription,
                        next: pending.next_subscription,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;

        let book_record = self.persist_record(
            pending.stamp,
            Record::Control(ControlRecord {
                context: pending.stamp.wire_context(self.active_context),
                value: Control::EpochAdvance {
                    change: EpochChange::Book {
                        owner: pending.book_id,
                        expected: old_book,
                        next: pending.next_book,
                    },
                    reason: Reason::Reconnect,
                },
            }),
            sink,
        )?;

        let new_tag = {
            let runtime =
                self.streams
                    .get_mut(&stream)
                    .ok_or(SupervisorError::InvalidConfiguration(
                        "missing disconnected stream",
                    ))?;
            runtime.previous_tag = Some(old_tag);
            runtime.terminal_down_epoch = None;
            runtime.binding.tag.connection = pending.next_connection;
            runtime.binding.tag.subscription = pending.next_subscription;
            runtime.binding.tag.book = Some(pending.next_book);
            runtime.transport = Transport::Unknown;
            runtime.subscription = SubscriptionState::Backoff;
            runtime.continuity.clear_for_new_generation();
            runtime.last_market_record = None;
            runtime.pending_disconnect = None;
            runtime.reconnect_failures = pending.reconnect_attempt;
            runtime.reconnect_not_before_ns = Some(pending.reconnect_not_before_ns);
            runtime.binding.tag
        };

        Ok(CoreDrainResult {
            records: vec![connection_record, subscription_record, book_record],
            commands: vec![TransportCommand::ReconnectAfter {
                connection,
                epoch: new_tag.connection,
                delay_ns: pending.delay_ns,
            }],
            events: vec![SupervisorEvent::EpochAdvanced {
                stream,
                records: vec![connection_record, subscription_record, book_record],
                tag: new_tag,
            }],
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
        self.ensure_record_capacity(1)?;
        let record_no = self
            .next_record_no
            .ok_or(SupervisorError::CounterExhausted("RecordNo"))?;
        let next_record_no = if self.authority_heartbeat {
            None
        } else {
            Some(match record_no.checked_next() {
                Ok(value) => value,
                Err(_) => return self.halt_with(SupervisorError::CounterExhausted("RecordNo")),
            })
        };
        let frame = RecordFrame {
            record_no,
            segment_no: self.segment_no,
            value,
        };
        let accounted = match &frame.value {
            Record::RawInput(raw) => Some((raw.stream, raw.attempt.get())),
            Record::Gap(gap) => match &gap.scope {
                GapScope::ExplicitTargets(targets) if targets.len() == 1 => targets[0]
                    .range
                    .map(|(_, last)| (targets[0].stream, last.get())),
                _ => None,
            },
            _ => None,
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
        if !receipt.achieved_gate.covers(self.recording_gate) {
            self.halted = true;
            return Err(SupervisorError::PersistenceGateTooWeak {
                required: self.recording_gate,
                achieved: receipt.achieved_gate,
            });
        }
        if let Some((stream, frontier)) = accounted
            && let Some(runtime) = self.streams.get_mut(&stream)
        {
            runtime.accounted_attempt_frontier = frontier;
        }
        self.next_record_no = Some(next_record_no.unwrap_or_else(|| {
            record_no
                .checked_next()
                .expect("bound receipt checked RecordNo")
        }));
        Ok(record_no)
    }

    fn stream_for_connection(&self, connection: ConnectionId) -> Result<StreamId, SupervisorError> {
        self.by_connection
            .get(&connection)
            .copied()
            .ok_or(SupervisorError::UnknownConnection(connection))
    }

    fn queue_gap_loss(
        &mut self,
        stream: StreamId,
        tag: EpochTag,
        attempt: CaptureAttemptNo,
        stamp: ReceiveStamp,
    ) -> Result<(), SupervisorError> {
        let tail_is_post_cut = self.cut_remaining.is_none()
            || self
                .cut_remaining
                .is_some_and(|remaining| self.queue.len() > remaining);
        if tail_is_post_cut
            && let Some(Ingress::QueueGap {
                stream: queued_stream,
                tag: queued_tag,
                last_attempt,
                loss_count,
                ..
            }) = self.queue.back_mut()
            && *queued_stream == stream
            && *queued_tag == tag
            && last_attempt.get().checked_add(1) == Some(attempt.get())
        {
            *last_attempt = attempt;
            *loss_count = loss_count
                .checked_add(1)
                .ok_or(SupervisorError::CounterExhausted("QueueGap.loss_count"))?;
            return Ok(());
        }

        self.push_loss_ingress(
            stream,
            Ingress::QueueGap {
                stream,
                tag,
                first_attempt: attempt,
                last_attempt: attempt,
                loss_count: 1,
                stamp,
            },
        )
    }

    /// An existing counted tail may represent this exact loss without a new
    /// admission owner. This preflight reads bytes only for the Pong delimiter;
    /// it does not decode or retain caller payload.
    fn received_loss_can_coalesce(
        &self,
        stream: StreamId,
        epoch: ConnectionEpoch,
        bytes: &[u8],
    ) -> bool {
        if bytes == b"pong" {
            return false;
        }
        let runtime = &self.streams[&stream];
        let Some((tag, current)) = runtime.tag_for_observed_connection(epoch) else {
            return false;
        };
        let Some(attempt) = runtime.capture_attempt_frontier.checked_add(1) else {
            return false;
        };
        let per_stream_room = runtime.queued_raw_frames
            < self.queue_policy.max_raw_frames_per_stream
            && runtime
                .queued_raw_bytes
                .checked_add(bytes.len())
                .is_some_and(|value| value <= self.queue_policy.max_raw_bytes_per_stream)
            && bytes.len() <= self.queue_policy.max_raw_message_bytes;
        let global_room = self.queued_raw_items < self.raw_item_limit
            && self.queue.len() + self.external_work < self.work_limit;
        let same_cut = self.cut_remaining.is_none()
            || self
                .cut_remaining
                .is_some_and(|remaining| self.queue.len() > remaining);
        current
            && !(per_stream_room && global_room)
            && same_cut
            && matches!(self.queue.back(), Some(Ingress::QueueGap {
                stream: queued_stream,
                tag: queued_tag,
                last_attempt,
                loss_count,
                ..
            }) if *queued_stream == stream
                && *queued_tag == tag
                && last_attempt.get().checked_add(1) == Some(attempt)
                && loss_count.checked_add(1).is_some())
    }

    fn push_loss_ingress(
        &mut self,
        stream: StreamId,
        ingress: Ingress,
    ) -> Result<(), SupervisorError> {
        self.push_ingress(stream, ingress)
    }

    fn push_ingress(&mut self, stream: StreamId, ingress: Ingress) -> Result<(), SupervisorError> {
        if self.queue.len() + self.external_work >= self.work_limit {
            return Err(SupervisorError::QueueExhausted { stream });
        }
        self.queue.push_back(ingress);
        Ok(())
    }

    fn checked_time_add_or_halt(&mut self, base: u64, delta: u64) -> Result<u64, SupervisorError> {
        match base.checked_add(delta) {
            Some(value) => Ok(value),
            None => self.halt_with(SupervisorError::TimeOverflow),
        }
    }

    fn ensure_record_capacity(&mut self, count: u64) -> Result<(), SupervisorError> {
        if self.authority_heartbeat {
            return Ok(());
        }
        let Some(record_no) = self.next_record_no else {
            return self.halt_with(SupervisorError::CounterExhausted("RecordNo"));
        };
        if record_no.get().checked_add(count).is_none() {
            return self.halt_with(SupervisorError::CounterExhausted("RecordNo"));
        }
        Ok(())
    }

    fn halt_with<T>(&mut self, error: SupervisorError) -> Result<T, SupervisorError> {
        self.halted = true;
        Err(error)
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

/// Fixed-capacity output storage. Its backing cannot grow while a result
/// retains a counted work owner. Conversion transfers data to caller storage.
pub struct BoundedList<T, const N: usize> {
    entries: [Option<T>; N],
    len: usize,
}
impl<T, const N: usize> Default for BoundedList<T, N> {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
        }
    }
}
impl<T, const N: usize> BoundedList<T, N> {
    fn from_vec(values: Vec<T>) -> Self {
        assert!(values.len() <= N, "bounded supervisor output plan");
        let mut result = Self::default();
        for value in values {
            result.entries[result.len] = Some(value);
            result.len += 1;
        }
        result
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().flatten()
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        self.entries.get(index).and_then(Option::as_ref)
    }
    pub fn into_vec(self) -> Vec<T> {
        self.into_iter().collect()
    }
}
impl<T, const N: usize> IntoIterator for BoundedList<T, N> {
    type Item = T;
    type IntoIter = std::iter::Flatten<std::array::IntoIter<Option<T>, N>>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter().flatten()
    }
}
impl<T: fmt::Debug, const N: usize> fmt::Debug for BoundedList<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
impl<T, const N: usize> std::ops::Index<usize> for BoundedList<T, N> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        self.get(index).expect("bounded list index")
    }
}

/// Admission is an observation, never a reusable publication permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    Admitted,
    CoalescedLoss,
    AlreadyTerminated,
}

pub struct AdmissionReport {
    pub session_disposition: domain::capture_session::SessionDisposition,
    pub scope_disposition: Option<domain::capture_session::ScopeDisposition>,
    pub outcome: Result<AdmissionOutcome, SupervisorError>,
    pub commands: BoundedList<domain::capture_session::CommandLease, MAX_CONFIGURED_STREAMS>,
    pub failure: Option<domain::capture_session::TerminalFailure>,
    pub close_owner: Option<domain::capture_session::CloseOwnerRef>,
    pub admitted_scopes: [Option<StreamId>; MAX_CONFIGURED_STREAMS],
    pub cancelled_plan: bool,
}

/// The private work token retains the same counted owner while a caller holds
/// its observation. Commands are affine authority-bound leases.
pub struct DrainResult {
    pub records: BoundedList<RecordNo, 3>,
    pub commands: BoundedList<domain::capture_session::CommandLease, 1>,
    pub events: BoundedList<SupervisorEvent, 4>,
    _owner: Option<domain::capture_session::WorkOwner>,
}

pub struct DrainReport {
    pub session_disposition: domain::capture_session::SessionDisposition,
    pub outcome: Result<Option<DrainResult>, SupervisorError>,
}

/// Sole canonical public supervisor. The protocol core and its interchangeable
/// sink adapter are private; accepted session authority authenticates every
/// mutation and every persist operation.
pub struct PublicWsSupervisor {
    core: SupervisorCore,
    handle: domain::capture_session::SupervisorSessionHandle,
    queued_owners: VecDeque<domain::capture_session::WorkOwner>,
    pending_owners: FixedRegistry<StreamId, domain::capture_session::WorkOwner>,
}

impl Drop for PublicWsSupervisor {
    fn drop(&mut self) {
        // Record/job stewardship is distinct from live Rust aliases. Notify
        // only the fixed W cells; the rightful turn performs failure/cut/Close
        // transitions later. No I/O, drain, allocation or RefCell borrow here.
        for owner in &self.queued_owners {
            owner.abandon_observation();
        }
        for owner in self.pending_owners.values() {
            owner.abandon_observation();
        }
    }
}

impl PublicWsSupervisor {
    pub fn new(
        config: WsSupervisorConfig,
        handle: session::SupervisorSessionHandle,
    ) -> Result<Self, SupervisorError> {
        let status = handle.authority().status();
        if status.failed {
            return Err(SupervisorError::Authority(
                session::AuthorityError::ArchiveFailed,
            ));
        }
        if status.storage_stopped.is_some() {
            return Err(SupervisorError::Authority(
                session::AuthorityError::StorageStopped,
            ));
        }
        if status.lifecycle != session::SessionLifecycle::Open {
            return Err(SupervisorError::Authority(
                if status.lifecycle == session::SessionLifecycle::Closing {
                    session::AuthorityError::SessionClosing
                } else {
                    session::AuthorityError::SessionClosed
                },
            ));
        }
        handle
            .validate_stream_bindings(&config.streams)
            .map_err(SupervisorError::Authority)?;
        let prefix = handle.prefix();
        let budget = handle.budget();
        if prefix.context != config.active_context
            || prefix.recording_gate != config.recording_gate
            || prefix.segment != config.segment_no
            || prefix.next_record != config.next_record_no
            || budget.item_cap != config.queue_policy.max_total_items
            || budget.raw_frame_limit != config.queue_policy.max_raw_frames_per_stream
            || budget.raw_byte_limit != config.queue_policy.max_raw_bytes_per_stream
            || budget.max_message_bytes != config.queue_policy.max_raw_message_bytes
        {
            return Err(SupervisorError::Authority(
                session::AuthorityError::InvalidBinding,
            ));
        }
        let bound = handle.scopes();
        if bound.iter().flatten().count() != config.streams.len()
            || config.streams.iter().any(|binding| {
                !bound.iter().flatten().any(|scope| {
                    scope.stream == binding.id
                        && scope.connection == binding.connection_id
                        && scope.epoch == binding.tag.connection
                })
            })
        {
            return Err(SupervisorError::Authority(
                session::AuthorityError::InvalidBinding,
            ));
        }
        let mut core = SupervisorCore::new(config)?;
        core.authority_heartbeat = true;
        let work_limit = core.work_limit;
        let supervisor = Self {
            core,
            handle,
            queued_owners: VecDeque::with_capacity(work_limit),
            pending_owners: FixedRegistry::new(),
        };
        let report = supervisor.checked_retention_report()?;
        let non_storage = checked_report_sum(&[
            report.ownership.metadata_ceiling_bytes,
            report.supervisor_metadata_ceiling_bytes,
            report.payload_ceiling_bytes,
            report.decoder_workspace_ceiling_bytes,
        ])?;
        if non_storage > usize::MAX / 2 {
            return Err(SupervisorError::Authority(
                session::AuthorityError::InvalidBudget,
            ));
        }
        Ok(supervisor)
    }

    fn synchronize_authority(
        &mut self,
        turn: &mut session::SessionTurn,
    ) -> Result<(), SupervisorError> {
        self.handle
            .authority()
            .synchronize_obligations(turn)
            .map_err(SupervisorError::Authority)?;
        let status = self.handle.authority().status();
        self.core.next_record_no = Some(self.handle.prefix().next_record);
        if status.failed {
            self.core.cut_remaining = Some(
                self.queued_owners
                    .iter()
                    .filter(|owner| owner.cut_side() == session::CutSide::PreCut)
                    .count(),
            );
        }
        self.core.marker_settled = matches!(status.marker, session::MarkerState::Confirmed(_));
        let streams: [Option<StreamId>; MAX_CONFIGURED_STREAMS] = std::array::from_fn(|index| {
            self.core
                .streams
                .entries
                .get(index)
                .map(|(stream, _)| *stream)
        });
        for stream in streams.into_iter().flatten() {
            if let Some(failure) = self.handle.authority().terminal_failure(stream) {
                let runtime = self.core.streams.get_mut(&stream).ok_or(
                    SupervisorError::InvalidConfiguration("missing terminal scope"),
                )?;
                runtime.capture_terminated = true;
                runtime.subscription = SubscriptionState::Degraded;
                runtime.last_market_record = None;
                match failure.attempt {
                    session::AttemptIdentity::Candidate(attempt) => {
                        runtime.capture_attempt_frontier =
                            runtime.capture_attempt_frontier.max(attempt.get())
                    }
                    session::AttemptIdentity::NoRepresentableSuccessor { frontier } => {
                        runtime.capture_attempt_frontier =
                            runtime.capture_attempt_frontier.max(frontier)
                    }
                    session::AttemptIdentity::NotRaw => {}
                }
                // A partial completion after a storage stop is an explicit
                // undrained owner, not an uncommitted plan we may settle away.
                if status.storage_stopped.is_none() {
                    self.cancel_pending_disconnect(turn, stream)?;
                }
            }
        }
        Ok(())
    }

    /// Settle only this scope's still-uncommitted generated obligation before
    /// removing either half of the plan/owner pair. Classification and Drop do
    /// not prove accounting. Any checked failure retains the plan and owner,
    /// including their already-confirmed Down and existing mandatory Close.
    fn cancel_pending_disconnect(
        &mut self,
        turn: &mut session::SessionTurn,
        stream: StreamId,
    ) -> Result<bool, SupervisorError> {
        self.handle
            .authority()
            .validate_turn(turn)
            .map_err(SupervisorError::Authority)?;
        let runtime =
            self.core
                .streams
                .get(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing cancellation scope",
                ))?;
        let pending = runtime.pending_disconnect;
        match (pending, self.pending_owners.get(&stream)) {
            (None, None) => return Ok(false),
            (Some(plan), Some(_)) if plan.epoch == runtime.binding.tag.connection => {}
            _ => {
                return Err(SupervisorError::InvalidConfiguration(
                    "pending disconnect owner mismatch",
                ));
            }
        }
        if self.handle.authority().status().storage_stopped.is_some() {
            return Err(SupervisorError::Authority(
                session::AuthorityError::StorageStopped,
            ));
        }
        let runtime =
            self.core
                .streams
                .get_mut(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing cancellation scope",
                ))?;
        let work =
            self.pending_owners
                .get(&stream)
                .ok_or(SupervisorError::InvalidConfiguration(
                    "missing pending cancellation owner",
                ))?;
        self.handle
            .cancel_generated_plan(turn, work)
            .map_err(SupervisorError::Authority)?;
        work.set_kind(turn, session::WorkKind::Command)
            .map_err(SupervisorError::Authority)?;
        // All fallible validation/settlement precedes either removal.
        runtime.pending_disconnect = None;
        self.pending_owners.remove(&stream);
        Ok(true)
    }

    pub fn snapshot(&self, stream: StreamId) -> Option<StreamSupervisorSnapshot> {
        self.core.snapshot(stream).map(|mut snapshot| {
            snapshot.session_disposition = Some(self.handle.authority().disposition());
            snapshot.first_failure = self.terminal_failure(stream);
            snapshot.capture_terminated = snapshot.first_failure.is_some();
            if let Some(failure) = snapshot.first_failure {
                match failure.attempt {
                    session::AttemptIdentity::Candidate(attempt) => {
                        snapshot.capture_attempt_frontier =
                            snapshot.capture_attempt_frontier.max(attempt.get())
                    }
                    session::AttemptIdentity::NoRepresentableSuccessor { frontier } => {
                        snapshot.capture_attempt_frontier =
                            snapshot.capture_attempt_frontier.max(frontier)
                    }
                    session::AttemptIdentity::NotRaw => {}
                }
            }
            snapshot
        })
    }
    pub fn queued_items(&self) -> usize {
        self.core.queued_items()
    }
    pub fn is_halted(&self) -> bool {
        self.core.is_halted() || self.handle.authority().status().storage_stopped.is_some()
    }
    pub fn session_status(&self) -> session::SessionStatus {
        self.handle.authority().status()
    }
    pub fn terminal_failure(&self, stream: StreamId) -> Option<session::TerminalFailure> {
        self.handle.authority().terminal_failure(stream)
    }
    pub fn quiesce(
        &mut self,
        turn: &mut session::SessionTurn,
        ticket: &session::CloseTicket,
    ) -> session::QuiescenceReport {
        let report = self.handle.quiesce(turn, ticket);
        if !matches!(report, session::QuiescenceReport::Rejected(_))
            && let Err(SupervisorError::Authority(error)) = self.synchronize_authority(turn)
        {
            return session::QuiescenceReport::Rejected(error);
        }
        report
    }

    pub fn retention_report(&self) -> SupervisorRetentionReport {
        // Construction checks every non-storage term and reserves half the
        // numeric range. Backend updates independently check their half before
        // mutation. Fixed registries/work slots and bounded payload capacities
        // cannot exceed that admitted profile during the session.
        self.checked_retention_report()
            .expect("validated retained byte budget")
    }

    pub fn checked_retention_report(&self) -> Result<SupervisorRetentionReport, SupervisorError> {
        let ownership = self.handle.authority().ownership_report();
        let mut raw = [RawRetention::default(); MAX_CONFIGURED_STREAMS];
        for (index, runtime) in self.core.streams.values().enumerate() {
            raw[index] = RawRetention {
                stream: Some(runtime.binding.id),
                frames: runtime.queued_raw_frames,
                payload_bytes: runtime.queued_raw_bytes,
                allocated_bytes: 0,
            };
        }
        for ingress in &self.core.queue {
            if let Ingress::Raw { stream, bytes, .. } = ingress
                && let Some(slot) = raw.iter_mut().find(|slot| slot.stream == Some(*stream))
            {
                slot.allocated_bytes =
                    checked_report_sum(&[slot.allocated_bytes, bytes.capacity()])?;
            }
        }
        let payload_ceiling_bytes = checked_report_product(
            self.core.streams.len(),
            self.core
                .queue_policy
                .max_raw_bytes_per_stream
                .min(checked_report_product(
                    self.core.queue_policy.max_raw_frames_per_stream,
                    self.core.queue_policy.max_raw_message_bytes,
                )?),
        )?;
        // Each fixed registry has <=4 entries. Stable registry allocation is
        // included in the bounded construction profile; work backing includes
        // both queue arrays and fixed per-job output capacities.
        let binding_bytes = self
            .core
            .streams
            .values()
            .try_fold(0usize, |sum, runtime| {
                checked_report_sum(&[sum, binding_text_bytes(&runtime.binding)])
            })?;
        let supervisor_metadata_backing_bytes = checked_report_sum(&[
            std::mem::size_of::<Self>(),
            checked_report_product(self.core.queue.capacity(), std::mem::size_of::<Ingress>())?,
            checked_report_product(
                self.queued_owners.capacity(),
                std::mem::size_of::<session::WorkOwner>(),
            )?,
            checked_report_product(
                self.core.streams.capacity(),
                std::mem::size_of::<(StreamId, StreamRuntime)>(),
            )?,
            checked_report_product(
                self.core.by_connection.capacity(),
                std::mem::size_of::<(ConnectionId, StreamId)>(),
            )?,
            checked_report_product(
                self.pending_owners.capacity(),
                std::mem::size_of::<(StreamId, session::WorkOwner)>(),
            )?,
            binding_bytes,
        ])?;
        let work_output_ceiling_bytes = checked_report_product(
            self.core.work_limit,
            checked_report_sum(&[
                std::mem::size_of::<DrainResult>(),
                checked_report_product(3, std::mem::size_of::<RecordNo>())?,
                4096,
            ])?,
        )?;
        let supervisor_metadata_ceiling_bytes =
            checked_report_sum(&[supervisor_metadata_backing_bytes, work_output_ceiling_bytes])?;
        // The accepted decoder uses bounded 1MiB message input, bounded parser
        // depth/items and bounded strings. Include Raw clone plus decode tree
        // and both source/sink encoding buffers in the workspace allowance.
        let message = self.core.queue_policy.max_raw_message_bytes;
        // One JSON value/key requires at least one input byte. Every container
        // edge therefore contributes at most one of message+1 logical slots.
        // Pinned std Vec growth is <=2*len+4 slots (u8 strings <=2*len+8).
        // Summing edges and all empty-container minima gives <=6*(P+1)
        // aggregate array/object slots. UTF-8 and numeric text capacity adds
        // <=10*P; decoded owned fields/frames and error text add <=16*P.
        // Four Raw representations cover queued backup, handler Raw clone,
        // input decoder and encoder staging; storage encoding is separately
        // reported by OwnershipReport.storage_memory.
        let largest_json_slot =
            std::mem::size_of::<JsonValue>().max(std::mem::size_of::<(String, JsonValue)>());
        let decoder_workspace_ceiling_bytes = checked_report_sum(&[
            checked_report_product(
                checked_report_product(6, checked_report_sum(&[message, 1])?)?,
                largest_json_slot,
            )?,
            checked_report_product(30, message)?,
            checked_report_product(1024, std::mem::size_of::<crate::WireTrade>())?,
            checked_report_product(100, std::mem::size_of::<crate::WireLevel>())?,
            checked_report_product(16, 1024)?,
        ])?;
        let storage = ownership.storage_memory;
        let metadata_ceiling_bytes = checked_report_sum(&[
            ownership.metadata_ceiling_bytes,
            storage.metadata_ceiling_bytes,
            supervisor_metadata_ceiling_bytes,
        ])?;
        let retained_bytes_ceiling = checked_report_sum(&[
            metadata_ceiling_bytes,
            payload_ceiling_bytes,
            decoder_workspace_ceiling_bytes,
            storage.workspace_ceiling_bytes,
            storage.backend_ceiling_bytes,
        ])?;
        let raw_backing = raw.iter().try_fold(0usize, |sum, raw| {
            checked_report_sum(&[sum, raw.allocated_bytes])
        })?;
        let reported_backing_bytes = checked_report_sum(&[
            ownership.metadata_backing_bytes,
            storage.metadata_backing_bytes,
            supervisor_metadata_backing_bytes,
            raw_backing,
            storage.workspace_backing_bytes,
            storage.backend_backing_bytes,
        ])?;
        Ok(SupervisorRetentionReport {
            ownership,
            raw,
            queued_work: self.queued_owners.len(),
            pending_work: self.pending_owners.len(),
            filled_terminal_slots: self
                .core
                .streams
                .values()
                .filter(|runtime| {
                    self.handle
                        .authority()
                        .terminal_failure(runtime.binding.id)
                        .is_some()
                })
                .count(),
            cut_remaining: if self.handle.authority().status().failed {
                Some(
                    self.queued_owners
                        .iter()
                        .filter(|owner| owner.cut_side() == session::CutSide::PreCut)
                        .count(),
                )
            } else {
                None
            },
            payload_ceiling_bytes,
            supervisor_metadata_backing_bytes,
            supervisor_metadata_ceiling_bytes,
            decoder_workspace_ceiling_bytes,
            metadata_ceiling_bytes,
            retained_bytes_ceiling,
            reported_backing_bytes,
        })
    }

    fn admission_report(
        &self,
        stream: Option<StreamId>,
        outcome: Result<AdmissionOutcome, SupervisorError>,
        commands: Vec<session::CommandLease>,
    ) -> AdmissionReport {
        AdmissionReport {
            session_disposition: self.handle.authority().disposition(),
            scope_disposition: stream
                .and_then(|stream| self.handle.authority().scope_disposition(stream).ok()),
            outcome,
            commands: BoundedList::from_vec(commands),
            failure: stream.and_then(|stream| self.terminal_failure(stream)),
            close_owner: None,
            admitted_scopes: [None; MAX_CONFIGURED_STREAMS],
            cancelled_plan: false,
        }
    }

    pub fn start_commands(&mut self, turn: &mut session::SessionTurn) -> AdmissionReport {
        let result = (|| {
            self.synchronize_authority(turn)?;
            if self.is_halted() {
                return Err(SupervisorError::Halted);
            }
            self.handle
                .authority()
                .ensure_admission_open(turn)
                .map_err(SupervisorError::Authority)?;
            if self.core.started {
                return Err(SupervisorError::AlreadyStarted);
            }
            let ownership = self.handle.authority().ownership_report();
            if ownership.work_limit - ownership.work_used < self.core.streams.len() {
                return Err(SupervisorError::Authority(
                    session::AuthorityError::WorkExhausted,
                ));
            }
            let mut work = Vec::with_capacity(self.core.streams.len());
            for _ in 0..self.core.streams.len() {
                work.push(
                    self.handle
                        .reserve_work(turn, session::WorkKind::Command)
                        .map_err(SupervisorError::Authority)?,
                );
            }
            let views = self.core.start_commands()?;
            let mut commands = Vec::with_capacity(views.len());
            for (view, owner) in views.into_iter().zip(work) {
                commands.extend(self.lease_command(turn, view, &owner)?);
            }
            Ok(commands)
        })();
        match result {
            Ok(commands) => self.admission_report(None, Ok(AdmissionOutcome::Admitted), commands),
            Err(error) => {
                if matches!(
                    error,
                    SupervisorError::Authority(session::AuthorityError::CounterExhausted(_))
                ) {
                    self.latch_hard_stop(turn);
                }
                self.admission_report(None, Err(error), Vec::new())
            }
        }
    }

    pub fn queue_text(
        &mut self,
        turn: &mut session::SessionTurn,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        bytes: &[u8],
    ) -> AdmissionReport {
        self.admit(
            turn,
            Some(connection),
            Some(epoch),
            stamp,
            ReceivedCall::Text(bytes),
        )
    }
    pub fn queue_connected(
        &mut self,
        turn: &mut session::SessionTurn,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> AdmissionReport {
        self.admit(
            turn,
            Some(connection),
            Some(epoch),
            stamp,
            ReceivedCall::Connected,
        )
    }
    pub fn queue_disconnected(
        &mut self,
        turn: &mut session::SessionTurn,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
    ) -> AdmissionReport {
        self.admit(
            turn,
            Some(connection),
            Some(epoch),
            stamp,
            ReceivedCall::Disconnected,
        )
    }
    pub fn queue_tick(
        &mut self,
        turn: &mut session::SessionTurn,
        stamp: ReceiveStamp,
    ) -> AdmissionReport {
        self.admit_due_timers(turn, stamp)
    }

    fn admit_due_timers(
        &mut self,
        turn: &mut session::SessionTurn,
        stamp: ReceiveStamp,
    ) -> AdmissionReport {
        let result = (|| {
            self.handle
                .authority()
                .validate_turn(turn)
                .map_err(SupervisorError::Authority)?;
            self.synchronize_authority(turn)?;
            self.core.ensure_started()?;
            self.handle
                .authority()
                .ensure_admission_open(turn)
                .map_err(SupervisorError::Authority)?;
            Ok(())
        })();
        if let Err(error) = result {
            return self.admission_report(None, Err(error), Vec::new());
        }
        let streams: [Option<StreamId>; MAX_CONFIGURED_STREAMS] =
            std::array::from_fn(|index| self.core.streams.keys().nth(index).copied());
        let mut admitted_scopes = [None; MAX_CONFIGURED_STREAMS];
        let mut admitted_count = 0;
        let mut outcome = Ok(AdmissionOutcome::Admitted);
        for stream in streams.into_iter().flatten() {
            if self.terminal_failure(stream).is_some() {
                continue;
            }
            let admission = self.handle.admit_due_timer(
                turn,
                stream,
                session::ReceiveStamp {
                    unix_ns: stamp.unix_ns,
                    monotonic_ns: stamp.monotonic_ns,
                },
            );
            let timer = match admission {
                Ok(session::TimerAdmission::Admitted(timer)) => timer,
                Ok(
                    session::TimerAdmission::NotDue | session::TimerAdmission::AlreadyQueued { .. },
                ) => continue,
                Err(error) => {
                    outcome = Err(SupervisorError::Authority(error));
                    break;
                }
            };
            let identity = timer.identity();
            let session::ObservationClass::Timer {
                timer_id,
                deadline_ns,
            } = identity.class
            else {
                unreachable!("authority admitted an original Timer")
            };
            let original_stamp = ReceiveStamp {
                unix_ns: identity.stamp.unix_ns,
                monotonic_ns: identity.stamp.monotonic_ns,
            };
            let ingress = match timer.kind() {
                session::TimerKind::Ping => Ingress::PingTimer {
                    stream: identity.stream,
                    epoch: identity.epoch,
                    stamp: original_stamp,
                    timer_id,
                    deadline_ns,
                },
                session::TimerKind::Timeout => Ingress::PongTimeout {
                    stream: identity.stream,
                    epoch: identity.epoch,
                    stamp: original_stamp,
                    timer_id,
                    deadline_ns,
                },
            };
            // Authority admission already reserved this exact W and ordinal.
            // Queue transfer has no fallible counter, mirror or second owner.
            self.core.queue.push_back(ingress);
            self.queued_owners.push_back(timer.into_owner());
            admitted_scopes[admitted_count] = Some(stream);
            admitted_count += 1;
        }
        let mut report = self.admission_report(None, outcome, Vec::new());
        report.admitted_scopes = admitted_scopes;
        report
    }

    fn admit(
        &mut self,
        turn: &mut session::SessionTurn,
        connection: Option<ConnectionId>,
        epoch: Option<ConnectionEpoch>,
        stamp: ReceiveStamp,
        call: ReceivedCall<'_>,
    ) -> AdmissionReport {
        self.admit_with_reserver(turn, connection, epoch, stamp, call, |handle, turn| {
            handle.reserve_work(turn, session::WorkKind::QueuedObservation)
        })
    }

    fn admit_with_reserver(
        &mut self,
        turn: &mut session::SessionTurn,
        connection: Option<ConnectionId>,
        epoch: Option<ConnectionEpoch>,
        stamp: ReceiveStamp,
        call: ReceivedCall<'_>,
        mut reserve: impl FnMut(
            &session::SupervisorSessionHandle,
            &mut session::SessionTurn,
        ) -> Result<session::WorkOwner, session::AuthorityError>,
    ) -> AdmissionReport {
        let stream =
            connection.and_then(|connection| self.core.by_connection.get(&connection).copied());
        if let Err(error) = self.handle.authority().validate_turn(turn) {
            return self.admission_report(
                stream,
                Err(SupervisorError::Authority(error)),
                Vec::new(),
            );
        }
        if let Err(error) = self.synchronize_authority(turn) {
            return self.admission_report(stream, Err(error), Vec::new());
        }
        if !self.core.started {
            return self.admission_report(stream, Err(SupervisorError::NotStarted), Vec::new());
        }
        if let Some(stream) = stream
            && self.terminal_failure(stream).is_some()
        {
            return self.admission_report(
                Some(stream),
                Ok(AdmissionOutcome::AlreadyTerminated),
                Vec::new(),
            );
        }
        if self.is_halted() {
            return self.admission_report(stream, Err(SupervisorError::Halted), Vec::new());
        }
        if let Err(error) = self.handle.authority().ensure_admission_open(turn) {
            return self.admission_report(
                stream,
                Err(SupervisorError::Authority(error)),
                Vec::new(),
            );
        }
        if let Some(connection) = connection
            && stream.is_none()
        {
            return self.admission_report(
                None,
                Err(SupervisorError::UnknownConnection(connection)),
                Vec::new(),
            );
        }
        let before = self.core.queue.len();
        let ownership = self.handle.authority().ownership_report();
        let free = ownership.work_limit - ownership.work_used;
        if let (Some(stream), Some(connection), Some(epoch)) = (stream, connection, epoch) {
            let runtime = &self.core.streams[&stream];
            let valid = match call {
                ReceivedCall::Text(bytes) if bytes != b"pong" => {
                    runtime.tag_for_observed_connection(epoch).is_some()
                }
                _ => runtime.binding.tag.connection == epoch,
            };
            if !valid {
                return self.admission_report(
                    Some(stream),
                    Err(SupervisorError::UnknownConnectionEpoch { connection, epoch }),
                    Vec::new(),
                );
            }
            if matches!(call, ReceivedCall::Connected)
                && let Some(not_before_ns) = runtime.reconnect_not_before_ns
                && stamp.monotonic_ns < not_before_ns
            {
                return self.admission_report(
                    Some(stream),
                    Err(SupervisorError::ReconnectTooEarly {
                        connection,
                        not_before_ns,
                        observed_ns: stamp.monotonic_ns,
                    }),
                    Vec::new(),
                );
            }
        }
        self.core.external_work = ownership.work_used - self.queued_owners.len();
        let reuses_loss_owner = match (stream, epoch, call) {
            (Some(stream), Some(epoch), ReceivedCall::Text(bytes)) => {
                self.core.received_loss_can_coalesce(stream, epoch, bytes)
            }
            _ => false,
        };
        let proposed = usize::from(!reuses_loss_owner);
        let stage_count = free.min(proposed);
        let mut staged = VecDeque::with_capacity(stage_count);
        for _ in 0..stage_count {
            match reserve(&self.handle, turn) {
                Ok(work) => staged.push_back(work),
                Err(error) => {
                    if matches!(
                        error,
                        session::AuthorityError::CounterExhausted("AdmissionOrder")
                    ) {
                        return self.install_received_failure(
                            turn,
                            stream.expect("validated received scope"),
                            epoch.expect("validated received epoch"),
                            stamp,
                            call,
                            SupervisorError::Authority(error),
                        );
                    }
                    let _ = self.handle.authority().hard_stop(
                        turn,
                        PersistError::typed(
                            session::PersistErrorKind::Counter,
                            "work admission order exhausted",
                        ),
                    );
                    return self.admission_report(
                        stream,
                        Err(SupervisorError::Authority(error)),
                        Vec::new(),
                    );
                }
            }
        }
        // Staged tokens already reserve the prospective jobs. The protocol
        // core's capacity check excludes them, but includes all held results,
        // pending plans and command leases from earlier jobs.
        let outcome = match call {
            ReceivedCall::Text(bytes) => self.core.queue_text(
                connection.expect("received connection"),
                epoch.expect("received epoch"),
                stamp,
                bytes,
            ),
            ReceivedCall::Connected => self.core.queue_connected(
                connection.expect("received connection"),
                epoch.expect("received epoch"),
                stamp,
            ),
            ReceivedCall::Disconnected => self.core.queue_disconnected(
                connection.expect("received connection"),
                epoch.expect("received epoch"),
                stamp,
            ),
        };
        let added = self.core.queue.len() - before;
        let mut admitted = [None; MAX_CONFIGURED_STREAMS];
        for (index, admitted_stream) in admitted.iter_mut().enumerate().take(added) {
            let ingress = &self.core.queue[before + index];
            let observed_stream = ingress.stream();
            let work = staged
                .pop_front()
                .expect("reserved work for admitted ingress");
            if let Err(error) =
                self.handle
                    .admit_observation(turn, &work, ingress.observation_identity())
            {
                // A checked identity-admission failure keeps the represented
                // input in the authority's terminal slot, not an unowned queue.
                while self.core.queue.len() > before {
                    if let Some(Ingress::Raw { stream, bytes, .. }) = self.core.queue.pop_back() {
                        let runtime = self.core.streams.get_mut(&stream).expect("received scope");
                        runtime.queued_raw_frames -= 1;
                        runtime.queued_raw_bytes -= bytes.len();
                        self.core.queued_raw_items -= 1;
                    }
                }
                drop(work);
                let mut report = self.admission_report(
                    Some(observed_stream),
                    Err(SupervisorError::Authority(error)),
                    Vec::new(),
                );
                if report.failure.is_some() {
                    report.close_owner = self
                        .handle
                        .authority()
                        .outstanding_close_owners()
                        .iter()
                        .find(|view| view.owner.stream() == observed_stream)
                        .map(|view| view.owner.clone());
                    if let Some(close) = report.close_owner.clone()
                        && let session::CloseLeaseReport::Leased(lease) =
                            self.handle.authority().reclaim_close(turn, close)
                        && let Ok(command) = lease.into_command()
                    {
                        report.commands = BoundedList::from_vec(vec![command]);
                    }
                }
                return report;
            }
            *admitted_stream = Some(observed_stream);
            self.queued_owners.push_back(work);
        }
        if added == 0 && reuses_loss_owner {
            let ingress = self.core.queue.back().expect("coalesced GAP tail");
            let work = self.queued_owners.back().expect("counted GAP tail");
            self.handle
                .extend_gap_observation(turn, work, ingress.observation_identity())
                .expect("core validated same-side GAP coalescing");
        }
        drop(staged);
        if let Err(
            ref error @ (SupervisorError::QueueExhausted { .. }
            | SupervisorError::CounterExhausted("CaptureAttemptNo")),
        ) = outcome
        {
            let mut report = self.install_received_failure(
                turn,
                stream.expect("known received scope"),
                epoch.expect("received epoch"),
                stamp,
                call,
                error.clone(),
            );
            report.admitted_scopes = admitted;
            return report;
        }
        if self.core.halted {
            self.latch_hard_stop(turn);
        }
        let mut report = self.admission_report(
            stream,
            outcome.map(|()| {
                if added == 0 && matches!(call, ReceivedCall::Text(bytes) if bytes != b"pong") {
                    AdmissionOutcome::CoalescedLoss
                } else {
                    AdmissionOutcome::Admitted
                }
            }),
            Vec::new(),
        );
        report.admitted_scopes = admitted;
        report
    }

    fn install_received_failure(
        &mut self,
        turn: &mut session::SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        stamp: ReceiveStamp,
        call: ReceivedCall<'_>,
        error: SupervisorError,
    ) -> AdmissionReport {
        let runtime = &self.core.streams[&stream];
        let observed_tag = runtime
            .tag_for_observed_connection(epoch)
            .map_or(runtime.binding.tag, |(tag, _)| tag);
        let attempt = if matches!(call, ReceivedCall::Text(bytes) if bytes != b"pong") {
            runtime
                .capture_attempt_frontier
                .checked_add(1)
                .and_then(|number| CaptureAttemptNo::new(number).ok())
                .map_or(
                    session::AttemptIdentity::NoRepresentableSuccessor {
                        frontier: runtime.capture_attempt_frontier,
                    },
                    session::AttemptIdentity::Candidate,
                )
        } else {
            session::AttemptIdentity::NotRaw
        };
        let failure = session::TerminalFailure {
            stream,
            connection: runtime.binding.connection_id,
            observed_tag,
            current_epoch: runtime.binding.tag.connection,
            context: self.core.active_context,
            stamp: session::ReceiveStamp {
                unix_ns: stamp.unix_ns,
                monotonic_ns: stamp.monotonic_ns,
            },
            input_class: match call {
                ReceivedCall::Text(bytes) if bytes == b"pong" => session::InputClass::Pong,
                ReceivedCall::Text(_) => session::InputClass::Raw,
                ReceivedCall::Connected => session::InputClass::Connected,
                ReceivedCall::Disconnected => session::InputClass::Disconnected,
            },
            attempt,
            cause: match error {
                SupervisorError::CounterExhausted("CaptureAttemptNo") => {
                    session::FailureCause::CaptureAttemptExhausted
                }
                SupervisorError::Authority(session::AuthorityError::CounterExhausted(name)) => {
                    session::FailureCause::CounterExhausted(name)
                }
                _ => session::FailureCause::QueueOverflow,
            },
        };
        match self.handle.terminate(turn, failure) {
            Ok(terminated) => {
                self.core.cut_remaining = Some(
                    self.queued_owners
                        .iter()
                        .filter(|owner| owner.cut_side() == session::CutSide::PreCut)
                        .count(),
                );
                let cancellation = self.cancel_pending_disconnect(turn, stream);
                let runtime = match self.core.streams.get_mut(&stream) {
                    Some(runtime) => runtime,
                    None => {
                        let commands = terminated
                            .close
                            .and_then(|lease| lease.into_command().ok())
                            .map(|command| vec![command])
                            .unwrap_or_default();
                        let mut report = self.admission_report(
                            Some(stream),
                            Err(SupervisorError::InvalidConfiguration(
                                "missing terminal scope",
                            )),
                            commands,
                        );
                        report.close_owner = Some(terminated.close_owner);
                        return report;
                    }
                };
                runtime.capture_terminated = true;
                runtime.subscription = SubscriptionState::Degraded;
                runtime.last_market_record = None;
                if let session::AttemptIdentity::Candidate(attempt) = attempt {
                    runtime.capture_attempt_frontier = attempt.get();
                }
                let commands = terminated
                    .close
                    .and_then(|lease| lease.into_command().ok())
                    .map(|command| vec![command])
                    .unwrap_or_default();
                let (outcome, cancelled_plan) = match cancellation {
                    Ok(cancelled) => (Err(error), cancelled),
                    Err(cancellation_error) => (Err(cancellation_error), false),
                };
                let mut report = self.admission_report(Some(stream), outcome, commands);
                report.close_owner = Some(terminated.close_owner);
                report.cancelled_plan = cancelled_plan;
                report
            }
            Err(error) => self.admission_report(
                Some(stream),
                Err(SupervisorError::Authority(error)),
                Vec::new(),
            ),
        }
    }

    fn latch_hard_stop(&mut self, turn: &mut session::SessionTurn) {
        let _ = self.handle.authority().hard_stop(
            turn,
            PersistError::typed(
                session::PersistErrorKind::Counter,
                "terminal supervisor hard stop",
            ),
        );
        if self.core.cut_remaining.is_none() {
            self.core.cut_remaining = Some(self.core.queue.len());
        }
    }

    fn lease_command(
        &self,
        turn: &mut session::SessionTurn,
        view: TransportCommand,
        work: &session::WorkOwner,
    ) -> Result<Option<session::CommandLease>, SupervisorError> {
        if matches!(&view, TransportCommand::SendText { text, .. } if text == "ping") {
            return match self.handle.take_timer_ping(turn, work) {
                Ok(command) => Ok(Some(command)),
                Err(
                    session::AuthorityError::CommandRevoked
                    | session::AuthorityError::PingAlreadyTaken,
                ) => Ok(None),
                Err(error) => Err(SupervisorError::Authority(error)),
            };
        }
        let (connection, epoch, kind) = match view {
            TransportCommand::Connect {
                connection,
                epoch,
                endpoint,
            } => (
                connection,
                epoch,
                session::CommandKind::Connect { endpoint },
            ),
            TransportCommand::SendText {
                connection,
                epoch,
                text,
            } => (connection, epoch, session::CommandKind::SendText { text }),
            TransportCommand::ReconnectAfter {
                connection,
                epoch,
                delay_ns,
            } => (
                connection,
                epoch,
                session::CommandKind::ReconnectAfter { delay_ns },
            ),
            TransportCommand::Close { connection, epoch } => {
                let stream = self.core.by_connection[&connection];
                let owner = self
                    .handle
                    .mandatory_close(turn, stream, epoch, Some(work))
                    .map_err(SupervisorError::Authority)?;
                return match self.handle.authority().reclaim_close(turn, owner) {
                    session::CloseLeaseReport::Leased(lease) => lease
                        .into_command()
                        .map(Some)
                        .map_err(SupervisorError::Authority),
                    session::CloseLeaseReport::AlreadyLeased
                    | session::CloseLeaseReport::AlreadySettled => Ok(None),
                    session::CloseLeaseReport::Rejected(error) => {
                        Err(SupervisorError::Authority(error))
                    }
                };
            }
        };
        let stream = self.core.by_connection[&connection];
        match self.handle.command(turn, stream, epoch, kind, work) {
            Ok(command) => Ok(Some(command)),
            Err(
                session::AuthorityError::CommandRevoked
                | session::AuthorityError::SessionClosing
                | session::AuthorityError::SessionClosed,
            ) => Ok(None),
            Err(error) => Err(SupervisorError::Authority(error)),
        }
    }

    pub fn drain_one(
        &mut self,
        turn: &mut session::SessionTurn,
        sink: &mut session::BoundRecordSink,
    ) -> DrainReport {
        let outcome = self.drain_bound(turn, sink);
        DrainReport {
            session_disposition: self.handle.authority().disposition(),
            outcome,
        }
    }

    fn drain_authority_timer(
        &mut self,
        turn: &mut session::SessionTurn,
        sink: &mut session::BoundRecordSink,
        owner: &session::WorkOwner,
        original: &Ingress,
    ) -> Result<CoreDrainResult, SupervisorError> {
        let identity = original.observation_identity();
        let session::ObservationClass::Timer {
            timer_id,
            deadline_ns,
        } = identity.class
        else {
            return Err(SupervisorError::Authority(
                session::AuthorityError::InvalidOwner,
            ));
        };
        let stream = identity.stream;
        let stamp = ReceiveStamp {
            unix_ns: identity.stamp.unix_ns,
            monotonic_ns: identity.stamp.monotonic_ns,
        };
        self.core.queue.pop_front().expect("queued original Timer");
        if let Some(remaining) = self.core.cut_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }
        let before = self
            .handle
            .timer_progress(turn, owner)
            .map_err(SupervisorError::Authority)?;
        let timer_confirmed = match before {
            session::TimerProgressView::Unselected => false,
            session::TimerProgressView::TimerOnlyPing {
                timer_confirmed, ..
            }
            | session::TimerProgressView::TimerOnlyObsolete { timer_confirmed }
            | session::TimerProgressView::TimerThenDown {
                timer_confirmed, ..
            } => timer_confirmed,
        };
        let mut result = CoreDrainResult::default();
        if !timer_confirmed {
            let mut adapter = BoundAdapter {
                turn,
                sink,
                error: None,
                owner: Some(owner),
            };
            let recorded = self.core.persist_timer_observation(
                stream,
                stamp,
                timer_id,
                deadline_ns,
                &mut adapter,
            );
            result = recorded.map_err(|error| map_boundary(error, adapter.error))?;
        }
        let progress = self
            .handle
            .timer_progress(turn, owner)
            .map_err(SupervisorError::Authority)?;
        match progress {
            session::TimerProgressView::TimerOnlyPing {
                timer_confirmed: true,
                ping_taken: false,
            } => {
                let connection = self.core.streams[&stream].binding.connection_id;
                result.commands.push(TransportCommand::SendText {
                    connection,
                    epoch: identity.epoch,
                    text: "ping".to_owned(),
                });
            }
            session::TimerProgressView::TimerOnlyPing {
                timer_confirmed: true,
                ping_taken: true,
            }
            | session::TimerProgressView::TimerOnlyObsolete {
                timer_confirmed: true,
            } => {}
            session::TimerProgressView::TimerThenDown {
                timer_confirmed: true,
                down_confirmed,
                ..
            } => {
                if !down_confirmed {
                    let was_down =
                        self.core.streams[&stream].terminal_down_epoch == Some(identity.epoch);
                    let mut adapter = BoundAdapter {
                        turn,
                        sink,
                        error: None,
                        owner: Some(owner),
                    };
                    let recorded = self.core.persist_disconnect_observation(
                        stream,
                        identity.epoch,
                        stamp,
                        None,
                        &mut adapter,
                    );
                    let down = recorded.map_err(|error| map_boundary(error, adapter.error))?;
                    result.records.extend(down.records);
                    result.commands.extend(down.commands);
                    result.events.extend(down.events);
                    // The authority has already frozen the timeout and confirmed
                    // its original Down/Close. Future H1 arithmetic is a later
                    // phase and cannot retract that accepted obligation.
                    if self.core.allow_generated_disconnect
                        && !was_down
                        && let Some(plan) = self.core.preflight_disconnect(
                            stream,
                            identity.epoch,
                            stamp,
                            0,
                            true,
                        )?
                    {
                        self.core.install_disconnect_plan(stream, plan)?;
                    }
                } else {
                    result.commands.push(TransportCommand::Close {
                        connection: self.core.streams[&stream].binding.connection_id,
                        epoch: identity.epoch,
                    });
                }
            }
            _ => {
                return Err(SupervisorError::Authority(
                    session::AuthorityError::NotQuiescent,
                ));
            }
        }
        Ok(result)
    }

    fn drain_bound(
        &mut self,
        turn: &mut session::SessionTurn,
        sink: &mut session::BoundRecordSink,
    ) -> Result<Option<DrainResult>, SupervisorError> {
        self.handle
            .authority()
            .validate_turn(turn)
            .map_err(SupervisorError::Authority)?;
        if !self.handle.authority().same_authority(sink.authority()) {
            return Err(SupervisorError::Authority(
                session::AuthorityError::AuthorityMismatch,
            ));
        }
        self.synchronize_authority(turn)?;
        if !self.core.started {
            return Err(SupervisorError::NotStarted);
        }
        if self.is_halted() {
            return Err(SupervisorError::Halted);
        }
        for (stream, runtime) in self.core.streams.iter_mut() {
            if let Some(pending) = runtime.pending_disconnect {
                runtime.close_settled = self
                    .handle
                    .authority()
                    .close_state(*stream, pending.epoch)
                    .ok()
                    == Some(session::CloseState::Settled);
            }
        }
        let status = self.session_status();
        self.core.allow_generated_disconnect = matches!(
            status.lifecycle,
            session::SessionLifecycle::Open | session::SessionLifecycle::FailedDiagnostic
        );
        if matches!(
            status.lifecycle,
            session::SessionLifecycle::DiagnosticClosing
                | session::SessionLifecycle::DiagnosticClosed
        ) {
            let streams: [Option<StreamId>; MAX_CONFIGURED_STREAMS] =
                std::array::from_fn(|index| {
                    self.core
                        .streams
                        .entries
                        .get(index)
                        .map(|(stream, _)| *stream)
                });
            for stream in streams.into_iter().flatten() {
                self.cancel_pending_disconnect(turn, stream)?;
            }
        }
        let marker_due =
            status.marker == session::MarkerState::Pending && self.core.cut_remaining == Some(0);
        if marker_due {
            let mut adapter = BoundAdapter {
                turn,
                sink,
                error: None,
                owner: None,
            };
            let observation = status
                .archive_observation
                .or_else(|| {
                    status
                        .first_failure
                        .map(|failure| session::ArchiveFailureObservation {
                            context: ReceiveStamp {
                                unix_ns: failure.stamp.unix_ns,
                                monotonic_ns: failure.stamp.monotonic_ns,
                            }
                            .wire_context(failure.context),
                            kind: match self.core.recording_gate {
                                RecordingGate::Written => WatermarkKind::Written,
                                RecordingGate::Flushed => WatermarkKind::Flushed,
                                RecordingGate::Durable => WatermarkKind::Durable,
                            },
                            reason: if failure.cause == session::FailureCause::QueueOverflow {
                                Reason::QueueOverflow
                            } else {
                                Reason::Unknown
                            },
                        })
                })
                .ok_or(SupervisorError::InvalidConfiguration(
                    "marker lacks archive failure observation",
                ))?;
            let stamp = ReceiveStamp {
                unix_ns: observation.context.unix_ns.get(),
                monotonic_ns: observation.context.monotonic_ns.get(),
            };
            let record = self.core.persist_record(
                stamp,
                Record::Control(ControlRecord {
                    context: observation.context,
                    value: Control::Recording(domain::record::RecordingEvidence {
                        health: domain::record::RecordingHealth::Failed,
                        kind: observation.kind,
                        through: self.handle.authority().trusted_watermark(observation.kind),
                        reason: observation.reason,
                    }),
                }),
                &mut adapter,
            );
            let boundary_error = adapter.error;
            let record = match record {
                Ok(record) => record,
                Err(error) => {
                    self.latch_hard_stop(turn);
                    return Err(map_boundary(error, boundary_error));
                }
            };
            self.handle
                .authority()
                .marker_confirmed(turn, record)
                .map_err(SupervisorError::Authority)?;
            self.core.marker_settled = true;
            return Ok(Some(DrainResult {
                records: BoundedList::from_vec(vec![record]),
                commands: BoundedList::default(),
                events: BoundedList::default(),
                _owner: None,
            }));
        }
        let completing = if self.core.cut_remaining.is_none() || self.core.marker_settled {
            self.core
                .streams
                .keys()
                .copied()
                .find(|stream| self.core.disconnect_ready(*stream))
        } else {
            None
        };
        let original = if completing.is_none() {
            self.core.queue.front().cloned()
        } else {
            None
        };
        let cut_before = self.core.cut_remaining;
        let retained_owner = if let Some(stream) = completing {
            self.pending_owners.get(&stream)
        } else {
            self.queued_owners.front()
        };
        if retained_owner.is_none() {
            return Ok(None);
        }
        // This existing completion boundary validates FIFO/frozen-plan order
        // before the private core's fallible operational preflight. It grants
        // no Timer plan; missing exact records remain NotQuiescent.
        let mut precompleted_obsolete = false;
        let mut precompleted_down = false;
        if let (Some(original), Some(owner)) = (original.as_ref(), retained_owner) {
            let identity = original.observation_identity();
            if matches!(
                identity.class,
                session::ObservationClass::Connected
                    | session::ObservationClass::Pong
                    | session::ObservationClass::Disconnected
            ) {
                let obsolete = matches!(
                    identity.class,
                    session::ObservationClass::Connected | session::ObservationClass::Pong
                )
                .then_some((identity.stream, identity.epoch));
                match self
                    .handle
                    .complete_observation(turn, sink, owner, obsolete)
                {
                    Ok(()) => {
                        precompleted_down =
                            identity.class == session::ObservationClass::Disconnected;
                        precompleted_obsolete = !precompleted_down;
                    }
                    Err(session::AuthorityError::NotQuiescent) => {}
                    Err(error) => return Err(SupervisorError::Authority(error)),
                }
            }
        }
        if let Some(owner) = retained_owner {
            owner
                .set_kind(
                    turn,
                    if completing.is_some() {
                        session::WorkKind::PendingPlan
                    } else {
                        session::WorkKind::InFlightObservation
                    },
                )
                .map_err(SupervisorError::Authority)?;
        }
        let owner = if let Some(stream) = completing {
            self.pending_owners.remove(&stream)
        } else {
            self.queued_owners.pop_front()
        };
        let halted_before = self.core.halted;
        let (result, boundary_error) = if precompleted_obsolete || precompleted_down {
            let original = self
                .core
                .queue
                .pop_front()
                .expect("obsolete original control");
            if let Some(remaining) = self.core.cut_remaining.as_mut() {
                *remaining = remaining.saturating_sub(1);
            }
            let identity = original.observation_identity();
            (
                Ok(Some(CoreDrainResult {
                    records: Vec::new(),
                    commands: if precompleted_down {
                        vec![TransportCommand::Close {
                            connection: self.core.streams[&identity.stream].binding.connection_id,
                            epoch: identity.epoch,
                        }]
                    } else {
                        Vec::new()
                    },
                    events: if precompleted_down {
                        Vec::new()
                    } else {
                        vec![SupervisorEvent::ObsoleteControl {
                            stream: identity.stream,
                            epoch: identity.epoch,
                        }]
                    },
                })),
                None,
            )
        } else if let (Some(original), Some(owner)) = (original.as_ref(), owner.as_ref())
            && matches!(
                original,
                Ingress::PingTimer { .. } | Ingress::PongTimeout { .. }
            )
        {
            let result = self
                .drain_authority_timer(turn, sink, owner, original)
                .map(Some);
            let boundary_error = result.as_ref().err().and_then(|error| {
                if let SupervisorError::Authority(error) = error {
                    Some(session::PersistBoundaryError::Authority(*error))
                } else {
                    None
                }
            });
            (result, boundary_error)
        } else {
            let mut adapter = BoundAdapter {
                turn,
                sink,
                error: None,
                owner: owner.as_ref(),
            };
            let result = self.core.drain_one(&mut adapter);
            (result, adapter.error)
        };
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if let Some(owner) = owner {
                    if let Some(stream) = completing {
                        self.pending_owners.insert(stream, owner);
                        if let Some(owner) = self.pending_owners.get(&stream) {
                            owner
                                .set_kind(turn, session::WorkKind::PendingPlan)
                                .map_err(SupervisorError::Authority)?;
                        }
                    } else if let Some(original) = original {
                        if let Ingress::Raw { stream, bytes, .. } = &original {
                            let runtime = self
                                .core
                                .streams
                                .get_mut(stream)
                                .expect("retained raw scope");
                            runtime.queued_raw_frames += 1;
                            runtime.queued_raw_bytes += bytes.len();
                            self.core.queued_raw_items += 1;
                        }
                        self.core.queue.push_front(original);
                        self.core.cut_remaining = cut_before;
                        self.queued_owners.push_front(owner);
                        if let Some(owner) = self.queued_owners.front() {
                            owner
                                .set_kind(turn, session::WorkKind::QueuedObservation)
                                .map_err(SupervisorError::Authority)?;
                        }
                    }
                }
                if matches!(
                    boundary_error,
                    Some(session::PersistBoundaryError::Authority(_))
                ) && self.handle.authority().status().storage_stopped.is_none()
                {
                    self.core.halted = halted_before;
                } else {
                    self.latch_hard_stop(turn);
                }
                return Err(map_boundary(error, boundary_error));
            }
        };
        let Some(result) = result else {
            return Ok(None);
        };
        let owner = owner.ok_or(SupervisorError::InvalidConfiguration(
            "drain without retained owner",
        ))?;
        let obsolete = result.events.iter().find_map(|event| {
            if let SupervisorEvent::ObsoleteControl { stream, epoch } = event {
                Some((*stream, *epoch))
            } else {
                None
            }
        });
        if !precompleted_obsolete
            && !precompleted_down
            && let Err(error) = self
                .handle
                .complete_observation(turn, sink, &owner, obsolete)
        {
            // A successful receipt does not surrender the original steward
            // when its remaining completion/Close validation rejects.
            if let Some(stream) = completing {
                owner
                    .set_kind(turn, session::WorkKind::PendingPlan)
                    .map_err(SupervisorError::Authority)?;
                self.pending_owners.insert(stream, owner);
            } else if let Some(original) = original {
                owner
                    .set_kind(turn, session::WorkKind::QueuedObservation)
                    .map_err(SupervisorError::Authority)?;
                self.core.queue.push_front(original);
                self.core.cut_remaining = cut_before;
                self.queued_owners.push_front(owner);
            }
            return Err(SupervisorError::Authority(error));
        }
        for event in &result.events {
            if let SupervisorEvent::EpochAdvanced { stream, tag, .. } = event {
                let previous = self.core.streams[stream]
                    .previous_tag
                    .expect("completed previous epoch")
                    .connection;
                self.handle
                    .authority()
                    .advance_epoch(turn, *stream, previous, tag.connection)
                    .map_err(SupervisorError::Authority)?;
            }
        }
        let mut commands = Vec::with_capacity(result.commands.len());
        for view in result.commands {
            commands.extend(self.lease_command(turn, view, &owner)?);
        }
        let down_stream = result
            .events
            .iter()
            .find_map(|event| {
                if let SupervisorEvent::TransportRecorded {
                    stream,
                    value: Transport::Down,
                    ..
                } = event
                {
                    Some(*stream)
                } else {
                    None
                }
            })
            .or_else(|| {
                precompleted_down.then(|| original.as_ref().expect("retried Down").stream())
            });
        if let Some(stream) = down_stream
            && self.core.streams[&stream].pending_disconnect.is_some()
            && !self.pending_owners.contains_key(&stream)
        {
            self.pending_owners
                .insert(stream, owner.share().map_err(SupervisorError::Authority)?);
        }
        let retains_plan = down_stream.is_some_and(|stream| {
            self.pending_owners
                .get(&stream)
                .is_some_and(|pending| pending.id() == owner.id())
        });
        owner
            .set_kind(
                turn,
                if retains_plan {
                    session::WorkKind::PendingPlan
                } else {
                    session::WorkKind::Result
                },
            )
            .map_err(SupervisorError::Authority)?;
        if retains_plan {
            self.handle
                .retain_generated_plan(turn, &owner)
                .map_err(SupervisorError::Authority)?;
        }
        Ok(Some(DrainResult {
            records: BoundedList::from_vec(result.records),
            commands: BoundedList::from_vec(commands),
            events: BoundedList::from_vec(result.events),
            _owner: Some(owner),
        }))
    }
}

#[derive(Clone, Copy)]
enum ReceivedCall<'a> {
    Text(&'a [u8]),
    Connected,
    Disconnected,
}

impl Ingress {
    fn observation_identity(&self) -> session::ObservationIdentity {
        let stream = self.stream();
        let (epoch, stamp, class, tag, attempts, loss_count) = match *self {
            Self::Raw {
                tag,
                attempt,
                stamp,
                ..
            } => (
                tag.connection,
                stamp,
                session::ObservationClass::Raw,
                Some(tag),
                Some((attempt, attempt)),
                None,
            ),
            Self::RejectedStaleRaw {
                tag,
                attempt,
                stamp,
                ..
            } => (
                tag.connection,
                stamp,
                session::ObservationClass::RejectedStaleRaw,
                Some(tag),
                Some((attempt, attempt)),
                None,
            ),
            Self::QueueGap {
                tag,
                first_attempt,
                last_attempt,
                loss_count,
                stamp,
                ..
            } => (
                tag.connection,
                stamp,
                session::ObservationClass::Gap,
                Some(tag),
                Some((first_attempt, last_attempt)),
                Some(loss_count),
            ),
            Self::Connected { epoch, stamp, .. } => (
                epoch,
                stamp,
                session::ObservationClass::Connected,
                None,
                None,
                None,
            ),
            Self::Disconnected { epoch, stamp, .. } => (
                epoch,
                stamp,
                session::ObservationClass::Disconnected,
                None,
                None,
                None,
            ),
            Self::Pong { epoch, stamp, .. } => (
                epoch,
                stamp,
                session::ObservationClass::Pong,
                None,
                None,
                None,
            ),
            Self::PingTimer {
                epoch,
                stamp,
                timer_id,
                deadline_ns,
                ..
            }
            | Self::PongTimeout {
                epoch,
                stamp,
                timer_id,
                deadline_ns,
                ..
            } => (
                epoch,
                stamp,
                session::ObservationClass::Timer {
                    timer_id,
                    deadline_ns,
                },
                None,
                None,
                None,
            ),
        };
        session::ObservationIdentity {
            stream,
            epoch,
            stamp: session::ReceiveStamp {
                unix_ns: stamp.unix_ns,
                monotonic_ns: stamp.monotonic_ns,
            },
            class,
            tag,
            attempts,
            loss_count,
        }
    }

    fn stream(&self) -> StreamId {
        match self {
            Self::Connected { stream, .. }
            | Self::Disconnected { stream, .. }
            | Self::Raw { stream, .. }
            | Self::RejectedStaleRaw { stream, .. }
            | Self::QueueGap { stream, .. }
            | Self::Pong { stream, .. }
            | Self::PingTimer { stream, .. }
            | Self::PongTimeout { stream, .. } => *stream,
        }
    }
}

struct BoundAdapter<'a, 'w> {
    turn: &'a mut session::SessionTurn,
    sink: &'a mut session::BoundRecordSink,
    error: Option<session::PersistBoundaryError>,
    owner: Option<&'w session::WorkOwner>,
}
impl RecordSink for BoundAdapter<'_, '_> {
    fn persist(
        &mut self,
        frame: &RecordFrame,
        gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
        let receipt = match self.owner {
            Some(owner) => self.sink.persist_owned(self.turn, frame, gate, owner),
            None => self.sink.persist_marker(self.turn, frame, gate),
        };
        match receipt {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.error = Some(error);
                Err(PersistError::typed(
                    session::PersistErrorKind::Adapter,
                    "bound persistence failed",
                ))
            }
        }
    }
}
fn map_boundary(
    fallback: SupervisorError,
    error: Option<session::PersistBoundaryError>,
) -> SupervisorError {
    match error {
        Some(session::PersistBoundaryError::Authority(session::AuthorityError::TimeOverflow)) => {
            SupervisorError::TimeOverflow
        }
        Some(session::PersistBoundaryError::Authority(error)) => SupervisorError::Authority(error),
        Some(session::PersistBoundaryError::Persistence(error)) => {
            SupervisorError::Persistence(error)
        }
        Some(session::PersistBoundaryError::ReceiptMismatch { expected, actual }) => {
            SupervisorError::PersistenceReceiptMismatch { expected, actual }
        }
        Some(session::PersistBoundaryError::WeakGate { required, achieved }) => {
            SupervisorError::PersistenceGateTooWeak { required, achieved }
        }
        None => fallback,
    }
}
fn checked_report_sum(values: &[usize]) -> Result<usize, SupervisorError> {
    values
        .iter()
        .try_fold(0usize, |sum, value| sum.checked_add(*value))
        .ok_or(SupervisorError::Authority(
            session::AuthorityError::InvalidBudget,
        ))
}

fn checked_report_product(a: usize, b: usize) -> Result<usize, SupervisorError> {
    a.checked_mul(b).ok_or(SupervisorError::Authority(
        session::AuthorityError::InvalidBudget,
    ))
}

fn binding_text_bytes(binding: &StreamBinding) -> usize {
    binding.spec.instrument.venue.as_str().len()
        + binding.spec.instrument.product_namespace.as_str().len()
        + binding.spec.instrument.native_symbol.as_str().len()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawRetention {
    pub stream: Option<StreamId>,
    pub frames: usize,
    pub payload_bytes: usize,
    pub allocated_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorRetentionReport {
    pub ownership: session::OwnershipReport,
    pub raw: [RawRetention; MAX_CONFIGURED_STREAMS],
    pub queued_work: usize,
    pub pending_work: usize,
    pub filled_terminal_slots: usize,
    pub cut_remaining: Option<usize>,
    pub payload_ceiling_bytes: usize,
    pub supervisor_metadata_backing_bytes: usize,
    pub supervisor_metadata_ceiling_bytes: usize,
    pub decoder_workspace_ceiling_bytes: usize,
    pub metadata_ceiling_bytes: usize,
    pub retained_bytes_ceiling: usize,
    pub reported_backing_bytes: usize,
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

#[cfg(test)]
mod tests {
    use super::*;
    use domain::identity::{
        ConfigVersion, FeedProfileVersion, InstrumentRef, InstrumentSlot, NormalizerVersion,
        SpecRef, SpecVersion, Token,
    };

    #[derive(Default)]
    struct MemorySink {
        frames: Vec<RecordFrame>,
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
                achieved_gate: required_gate,
            })
        }
    }

    fn fixture() -> (SupervisorCore, StreamBinding, MemorySink) {
        let spec = SpecVersion::new(1).expect("spec");
        let binding = StreamBinding {
            id: StreamId::new(1).expect("stream"),
            instrument_slot: InstrumentSlot::new(1).expect("slot"),
            spec: SpecRef {
                instrument: InstrumentRef {
                    venue: Token::new("bitget").expect("venue"),
                    market: MarketKind::Perpetual,
                    product_namespace: Token::new("usdt-futures").expect("namespace"),
                    native_symbol: Token::new("BTCUSDT").expect("symbol"),
                },
                version: spec,
            },
            connection_id: ConnectionId::new(1).expect("connection"),
            channel: Channel::BookNormal,
            book_id: Some(BookId::new(1).expect("book")),
            tag: EpochTag {
                spec,
                connection: ConnectionEpoch::new(1).expect("connection epoch"),
                subscription: SubscriptionEpoch::new(1).expect("subscription epoch"),
                book: Some(BookEpoch::new(1).expect("book epoch")),
            },
            feed_profile: FeedProfileVersion::new(1).expect("profile"),
        };
        let mut supervisor = SupervisorCore::new(WsSupervisorConfig {
            active_context: ActiveContext {
                config: ConfigVersion::new(1).expect("config"),
                normalizer: NormalizerVersion::new(1).expect("normalizer"),
            },
            recording_gate: RecordingGate::Durable,
            segment_no: SegmentNo::new(0),
            next_record_no: RecordNo::new(5).expect("record"),
            queue_policy: QueuePolicy::default(),
            streams: vec![binding.clone()],
        })
        .expect("supervisor");
        supervisor.start_commands().expect("start");
        supervisor
            .queue_connected(binding.connection_id, binding.tag.connection, stamp(0))
            .expect("connected ingress");
        let mut sink = MemorySink::default();
        drain_all(&mut supervisor, &mut sink);
        (supervisor, binding, sink)
    }

    fn stamp(monotonic_ns: u64) -> ReceiveStamp {
        ReceiveStamp {
            unix_ns: 1_800_000_000_000_000_000,
            monotonic_ns,
        }
    }

    fn ack() -> Vec<u8> {
        concat!(
            r#"{"event":"subscribe","arg":{"instType":"usdt-futures","#,
            r#""topic":"books50","symbol":"BTCUSDT"}}"#,
        )
        .as_bytes()
        .to_vec()
    }

    fn drain_all(supervisor: &mut SupervisorCore, sink: &mut MemorySink) {
        while let Some(result) = supervisor.drain_one(sink).expect("drain") {
            for command in result.commands {
                if let TransportCommand::Close { connection, .. } = command {
                    let stream = supervisor.by_connection[&connection];
                    supervisor
                        .streams
                        .get_mut(&stream)
                        .expect("stream")
                        .close_settled = true;
                }
            }
        }
    }

    fn advance(supervisor: &mut SupervisorCore, binding: &StreamBinding, sink: &mut MemorySink) {
        supervisor
            .queue_disconnected(binding.connection_id, binding.tag.connection, stamp(10))
            .expect("disconnect ingress");
        drain_all(supervisor, sink);
        let snapshot = supervisor.snapshot(binding.id).expect("snapshot");
        assert_eq!(snapshot.tag.connection.get(), 2);
    }

    fn set_frontier(supervisor: &mut SupervisorCore, stream: StreamId, frontier: u64) {
        let runtime = supervisor.streams.get_mut(&stream).expect("runtime");
        runtime.capture_attempt_frontier = frontier;
    }

    #[test]
    fn checked_capture_attempt_boundary_preserves_admitted_max_without_wrap() {
        for previous_generation in [false, true] {
            let (mut supervisor, binding, mut sink) = fixture();
            if previous_generation {
                advance(&mut supervisor, &binding, &mut sink);
            }
            let current = supervisor
                .snapshot(binding.id)
                .expect("snapshot")
                .tag
                .connection;
            let observed = if previous_generation {
                binding.tag.connection
            } else {
                current
            };
            set_frontier(&mut supervisor, binding.id, u64::MAX - 1);
            supervisor
                .queue_text(binding.connection_id, current, stamp(20), ack())
                .expect("MAX ingress");
            let before = supervisor.snapshot(binding.id).expect("snapshot");
            assert_eq!(before.capture_attempt_frontier, u64::MAX);
            assert_eq!(
                supervisor.queue_text(binding.connection_id, observed, stamp(21), ack()),
                Err(SupervisorError::CounterExhausted("CaptureAttemptNo"))
            );
            assert!(!supervisor.is_halted());
            assert_eq!(supervisor.snapshot(binding.id).expect("snapshot"), before);
            assert!(
                matches!(supervisor.queue.front(), Some(Ingress::Raw { attempt, .. }) if attempt.get() == u64::MAX)
            );
            // The canonical wrapper installs the reserved terminal owner; this
            // private counter test proves the protocol core does not fabricate
            // MAX+1 or retract the previously admitted final attempt.
            drain_all(&mut supervisor, &mut sink);
            assert_eq!(
                supervisor
                    .snapshot(binding.id)
                    .expect("snapshot")
                    .accounted_attempt_frontier,
                u64::MAX
            );
        }
    }

    #[test]
    fn near_max_capture_attempt_progression_is_archive_long_across_generation_change() {
        let (mut supervisor, binding, mut sink) = fixture();
        set_frontier(&mut supervisor, binding.id, u64::MAX - 3);
        supervisor
            .queue_text(
                binding.connection_id,
                binding.tag.connection,
                stamp(1),
                ack(),
            )
            .expect("near-max current raw");
        drain_all(&mut supervisor, &mut sink);
        advance(&mut supervisor, &binding, &mut sink);
        let snapshot = supervisor.snapshot(binding.id).expect("snapshot");
        assert_eq!(snapshot.capture_attempt_frontier, u64::MAX - 2);
        supervisor
            .queue_text(
                binding.connection_id,
                binding.tag.connection,
                stamp(11),
                ack(),
            )
            .expect("near-max previous-generation raw");
        drain_all(&mut supervisor, &mut sink);
        let snapshot = supervisor.snapshot(binding.id).expect("snapshot");
        let current_epoch = snapshot.tag.connection;
        supervisor
            .queue_connected(
                binding.connection_id,
                current_epoch,
                stamp(RECONNECT_MAX_NS_V1 + 20),
            )
            .expect("connected after backoff");
        drain_all(&mut supervisor, &mut sink);
        supervisor
            .queue_text(
                binding.connection_id,
                current_epoch,
                stamp(RECONNECT_MAX_NS_V1 + 21),
                ack(),
            )
            .expect("last representable current raw");
        drain_all(&mut supervisor, &mut sink);
        let attempts: Vec<_> = sink
            .frames
            .iter()
            .filter_map(|frame| match &frame.value {
                Record::RawInput(raw) => Some((raw.attempt.get(), raw.tag.connection.get())),
                _ => None,
            })
            .collect();
        assert_eq!(
            attempts,
            [(u64::MAX - 2, 1), (u64::MAX - 1, 1), (u64::MAX, 2)]
        );
        assert!(!supervisor.is_halted());
        let snapshot = supervisor.snapshot(binding.id).expect("snapshot");
        assert_eq!(snapshot.capture_attempt_frontier, u64::MAX);
        assert_eq!(supervisor.queued_items(), 0);
        let durable_prefix = sink.frames.clone();
        assert_eq!(
            supervisor.queue_text(
                binding.connection_id,
                current_epoch,
                stamp(RECONNECT_MAX_NS_V1 + 22),
                ack(),
            ),
            Err(SupervisorError::CounterExhausted("CaptureAttemptNo"))
        );
        assert!(!supervisor.is_halted());
        assert_eq!(supervisor.drain_one(&mut sink), Ok(None));
        assert_eq!(sink.frames, durable_prefix);
        let snapshot = supervisor.snapshot(binding.id).expect("snapshot");
        assert_eq!(snapshot.capture_attempt_frontier, u64::MAX);
    }
    #[test]
    fn disconnect_record_counter_preflight_never_writes_partial_terminal_down() {
        for next in [u64::MAX - 1, u64::MAX - 4] {
            let (mut core, binding, mut sink) = fixture();
            core.next_record_no = Some(RecordNo::new(next).expect("near-max record"));
            core.queue_connected(binding.connection_id, binding.tag.connection, stamp(10))
                .expect("admitted Up");
            let up = core
                .drain_one(&mut sink)
                .expect("Up durable")
                .expect("Up result");
            assert_eq!(up.records, [RecordNo::new(next).expect("record")]);
            let before = core.snapshot(binding.id).expect("snapshot");
            let prefix = sink.frames.clone();
            core.queue_disconnected(binding.connection_id, binding.tag.connection, stamp(11))
                .expect("admitted Down");
            assert_eq!(
                core.drain_one(&mut sink),
                Err(SupervisorError::CounterExhausted("RecordNo"))
            );
            assert!(core.is_halted());
            assert_eq!(core.snapshot(binding.id).expect("snapshot"), before);
            assert_eq!(sink.frames, prefix);
            for _ in 0..2 {
                assert_eq!(core.drain_one(&mut sink), Err(SupervisorError::Halted));
                assert_eq!(sink.frames, prefix);
                assert_eq!(core.snapshot(binding.id).expect("snapshot"), before);
            }
            assert!(sink.frames.iter().all(|frame| !matches!(
                &frame.value,
                Record::Control(ControlRecord {
                    value: Control::Transport {
                        value: Transport::Down,
                        ..
                    },
                    ..
                })
            )));
        }
    }

    fn json_backing(value: &JsonValue) -> usize {
        match value {
            JsonValue::Array(values) => {
                values.capacity() * std::mem::size_of::<JsonValue>()
                    + values.iter().map(json_backing).sum::<usize>()
            }
            JsonValue::Object(values) => {
                values.capacity() * std::mem::size_of::<(String, JsonValue)>()
                    + values
                        .iter()
                        .map(|(key, value)| key.capacity() + json_backing(value))
                        .sum::<usize>()
            }
            JsonValue::String(value) | JsonValue::Number(value) => value.capacity(),
            _ => 0,
        }
    }

    #[test]
    fn adversarial_dense_and_nested_parser_backing_fits_derived_workspace() {
        let (core, _, _) = fixture();
        let message = core.queue_policy.max_raw_message_bytes;
        let largest_slot =
            std::mem::size_of::<JsonValue>().max(std::mem::size_of::<(String, JsonValue)>());
        let ceiling = 6 * (message + 1) * largest_slot + 30 * message;
        let limits = ParserLimits {
            max_nesting_depth: 16,
            max_container_items: 2048,
            max_string_bytes: 4096,
        };
        let dense = format!(
            "[{}]",
            std::iter::repeat_n("[0]", 2048)
                .collect::<Vec<_>>()
                .join(",")
        );
        let objects = format!(
            "[{}]",
            std::iter::repeat_n(r#"{"k":[0,0,0,0]}"#, 1024)
                .collect::<Vec<_>>()
                .join(",")
        );
        let nested = format!("{}0{}", "[".repeat(15), "]".repeat(15));
        for bytes in [dense, objects, nested] {
            assert!(bytes.len() <= message);
            let parsed = parse_json(bytes.as_bytes(), limits).expect("bounded adversarial JSON");
            assert!(json_backing(&parsed) <= ceiling);
        }
    }

    fn bound_boundary_fixture() -> (
        PublicWsSupervisor,
        recording::CaptureSessionOwner,
        session::SessionTurn,
        session::BoundRecordSink,
        Vec<StreamBinding>,
        std::path::PathBuf,
    ) {
        bound_boundary_fixture_with_policy(QueuePolicy::default())
    }

    fn bound_boundary_fixture_with_policy(
        policy: QueuePolicy,
    ) -> (
        PublicWsSupervisor,
        recording::CaptureSessionOwner,
        session::SessionTurn,
        session::BoundRecordSink,
        Vec<StreamBinding>,
        std::path::PathBuf,
    ) {
        let (config, handle, owner, turn, sink, bindings, path) =
            unconstructed_boundary_fixture(policy);
        let supervisor = PublicWsSupervisor::new(config, handle).expect("bound supervisor");
        (supervisor, owner, turn, sink, bindings, path)
    }

    fn unconstructed_boundary_fixture(
        policy: QueuePolicy,
    ) -> (
        WsSupervisorConfig,
        session::SupervisorSessionHandle,
        recording::CaptureSessionOwner,
        session::SessionTurn,
        session::BoundRecordSink,
        Vec<StreamBinding>,
        std::path::PathBuf,
    ) {
        let gate = RecordingGate::Durable;
        use domain::identity::{ArchiveId, CaptureSessionId, ClockId};
        use domain::numeric::ExactDecimal;
        use domain::policy::{DurabilityMode, PolicyFields, SilenceRule};
        use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
        use domain::record::{
            ArchiveStart, ConfigDefinition, InstrumentSpecRecord, ProvenanceKind, StreamDefinition,
        };
        let (core, first, _) = fixture();
        let mut second = first.clone();
        second.id = StreamId::new(2).expect("stream");
        second.instrument_slot = InstrumentSlot::new(2).expect("slot");
        second.connection_id = ConnectionId::new(2).expect("connection");
        second.book_id = Some(BookId::new(2).expect("book"));
        second.spec.instrument.native_symbol = Token::new("ETHUSDT").expect("symbol");
        let bindings = vec![first, second];
        let proof = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse::<domain::artifact::ArtifactRef>()
            .expect("proof");
        let bootstrap_stamp = |number: u64| WireContext {
            unix_ns: LocalUnixNs::new(number as i64),
            monotonic_ns: MonotonicNs::new(number),
            context: InputContext::Bootstrap,
        };
        let mut frames = vec![RecordFrame {
            record_no: RecordNo::new(1).expect("record"),
            segment_no: SegmentNo::new(0),
            value: Record::ArchiveStart(ArchiveStart {
                archive: ArchiveId::new([7; 16]).expect("archive"),
                session: CaptureSessionId::new([8; 16]).expect("session"),
                clock: ClockId::new(1).expect("clock"),
                mode: DurabilityMode::Buffered,
                previous_archive: None,
            }),
        }];
        for binding in &bindings {
            let number = frames.len() as u64 + 1;
            let numeric = NumericSpec::new(NumericSpecFields {
                reference: binding.spec.clone(),
                price_units: PriceUnits {
                    quote: Token::new("USDT").expect("quote"),
                    basis: Token::new("BTC").expect("basis"),
                },
                quantity_unit: Token::new("BTC").expect("qty"),
                base_asset: Token::new("BTC").expect("base"),
                price_increment: ExactDecimal::ONE,
                quantity_increment: ExactDecimal::ONE,
                quantity_to_base_multiplier: Some(ExactDecimal::ONE),
            })
            .expect("numeric");
            frames.push(RecordFrame {
                record_no: RecordNo::new(number).expect("record"),
                segment_no: SegmentNo::new(0),
                value: Record::InstrumentSpec(InstrumentSpecRecord {
                    context: bootstrap_stamp(number),
                    slot: binding.instrument_slot,
                    numeric,
                    provenance: proof,
                }),
            });
            frames.push(RecordFrame {
                record_no: RecordNo::new(number + 1).expect("record"),
                segment_no: SegmentNo::new(0),
                value: Record::StreamDefinition(StreamDefinition {
                    context: bootstrap_stamp(number + 1),
                    binding: binding.clone(),
                    provenance: proof,
                }),
            });
        }
        frames.push(RecordFrame {
            record_no: RecordNo::new(6).expect("record"),
            segment_no: SegmentNo::new(0),
            value: Record::ConfigDefinition(ConfigDefinition {
                context: bootstrap_stamp(6),
                next: core.active_context,
                provenance_kind: ProvenanceKind::Synthetic,
                evidence: proof,
                fields: PolicyFields {
                    silence_rule: SilenceRule::UnknownOnSilence,
                    freshness_deadline_ns: Some(1_000_000_000),
                    warmup_min_updates: Some(1),
                    warmup_min_elapsed_ns: Some(0),
                    allow_quiet_with_proof: false,
                    require_two_sided_snapshot: true,
                    recording_gate: gate,
                },
            }),
        });
        static NEXT_BOUNDARY_WAL: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let mut created = None;
        // The recording owner exclusively creates fresh storage. Fixture
        // selection retries only a bounded number of preexisting names;
        // filesystem cleanup belongs to the test runner's scratch lifecycle.
        for _ in 0..10_000 {
            let serial = NEXT_BOUNDARY_WAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "rec001d-boundary-{}-{serial}.wal",
                policy.max_total_items
            ));
            match recording::CaptureSessionOwner::create_new(
                &path,
                &frames[0],
                recording::BoundedCaptureProfile::new(&frames[1..]),
            ) {
                Ok((owner, turn)) => {
                    created = Some((owner, turn, path));
                    break;
                }
                Err(recording::OwnerError::Create(error))
                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    continue;
                }
                Err(error) => panic!("accepted fresh owner: {error}"),
            }
        }
        let (mut owner, mut turn, path) = created.expect("bounded fresh fixture name");
        let scopes: Vec<_> = bindings
            .iter()
            .map(|binding| session::ScopeBinding {
                stream: binding.id,
                connection: binding.connection_id,
                epoch: binding.tag.connection,
            })
            .collect();
        let (handle, sink) = owner
            .register_supervisor(
                &mut turn,
                &scopes,
                session::RetentionBudget {
                    item_cap: policy.max_total_items,
                    raw_frame_limit: policy.max_raw_frames_per_stream,
                    raw_byte_limit: policy.max_raw_bytes_per_stream,
                    max_message_bytes: policy.max_raw_message_bytes,
                },
                HeartbeatPolicy::SupervisorV2,
            )
            .expect("bound registration");
        let config = WsSupervisorConfig {
            active_context: core.active_context,
            recording_gate: gate,
            segment_no: SegmentNo::new(0),
            next_record_no: RecordNo::new(7).expect("next"),
            queue_policy: policy,
            streams: bindings.clone(),
        };
        (config, handle, owner, turn, sink, bindings, path)
    }

    fn dispatch_boundary(
        owner: &mut recording::CaptureSessionOwner,
        turn: &mut session::SessionTurn,
        commands: impl IntoIterator<Item = session::CommandLease>,
    ) {
        for command in commands {
            assert!(matches!(
                owner.dispatch(turn, command, |_| Ok::<(), ()>(())),
                session::DispatchReport::Dispatched
            ));
        }
    }

    struct PureTimerBoundarySink {
        frames: std::rc::Rc<std::cell::RefCell<Vec<RecordFrame>>>,
    }

    impl session::SessionRecordWriter for PureTimerBoundarySink {
        fn persist(
            &mut self,
            frame: &RecordFrame,
            gate: RecordingGate,
        ) -> Result<PersistenceReceipt, PersistError> {
            self.frames.borrow_mut().push(frame.clone());
            Ok(PersistenceReceipt {
                through: frame.record_no,
                achieved_gate: gate,
            })
        }
    }

    struct PureTimerBoundary {
        config: WsSupervisorConfig,
        handle: session::SupervisorSessionHandle,
        authority: session::CaptureSessionAuthority,
        turn: session::SessionTurn,
        sink: session::BoundRecordSink,
        bindings: Vec<StreamBinding>,
        frames: std::rc::Rc<std::cell::RefCell<Vec<RecordFrame>>>,
    }

    fn pure_timer_boundary_fixture(gate: RecordingGate) -> PureTimerBoundary {
        use domain::identity::{ArchiveId, CaptureSessionId, ClockId};
        let (core, first, _) = fixture();
        let mut second = first.clone();
        second.id = StreamId::new(2).unwrap();
        second.instrument_slot = InstrumentSlot::new(2).unwrap();
        second.connection_id = ConnectionId::new(2).unwrap();
        second.book_id = Some(BookId::new(2).unwrap());
        second.spec.instrument.native_symbol = Token::new("ETHUSDT").unwrap();
        let bindings = vec![first, second];
        let policy = QueuePolicy::default();
        let (authority, mut turn) =
            session::CaptureSessionAuthority::new(session::SessionBinding {
                archive: ArchiveId::new([7; 16]).unwrap(),
                session: CaptureSessionId::new([8; 16]).unwrap(),
                clock: ClockId::new(1).unwrap(),
            });
        let scopes: Vec<_> = bindings
            .iter()
            .map(|binding| session::ScopeBinding {
                stream: binding.id,
                connection: binding.connection_id,
                epoch: binding.tag.connection,
            })
            .collect();
        let next_record = RecordNo::new(7).unwrap();
        let handle = authority
            .register_supervisor(
                &mut turn,
                &scopes,
                session::RetentionBudget {
                    item_cap: policy.max_total_items,
                    raw_frame_limit: policy.max_raw_frames_per_stream,
                    raw_byte_limit: policy.max_raw_bytes_per_stream,
                    max_message_bytes: policy.max_raw_message_bytes,
                },
                session::PrefixBinding {
                    context: core.active_context,
                    recording_gate: gate,
                    segment: SegmentNo::new(0),
                    next_record,
                },
                HeartbeatPolicy::SupervisorV2,
            )
            .unwrap();
        authority
            .set_accepted_stream_bindings(&mut turn, &bindings)
            .unwrap();
        let frames = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let sink = authority
            .bind_sink(
                &mut turn,
                Box::new(PureTimerBoundarySink {
                    frames: std::rc::Rc::clone(&frames),
                }),
            )
            .unwrap();
        PureTimerBoundary {
            config: WsSupervisorConfig {
                active_context: core.active_context,
                recording_gate: gate,
                segment_no: SegmentNo::new(0),
                next_record_no: next_record,
                queue_policy: policy,
                streams: bindings.clone(),
            },
            handle,
            authority,
            turn,
            sink,
            bindings,
            frames,
        }
    }

    fn timer_a_authority_scheduler_order_retry(gate: RecordingGate) {
        // This is a pure trusted boundary test, with no storage guarantee.
        // Paired integration tests verify actual owner/WAL rejection bytes.
        // The authority's accepted Up is deliberately absent from the core.
        for with_cut in [false, true] {
            let PureTimerBoundary {
                mut config,
                handle,
                authority,
                mut turn,
                mut sink,
                bindings,
                frames,
            } = pure_timer_boundary_fixture(gate);
            let a = &bindings[0];
            let up_owner = handle
                .reserve_work(&mut turn, session::WorkKind::QueuedObservation)
                .unwrap();
            handle
                .admit_observation(
                    &mut turn,
                    &up_owner,
                    session::ObservationIdentity {
                        stream: a.id,
                        epoch: a.tag.connection,
                        stamp: session::ReceiveStamp {
                            unix_ns: stamp(1).unix_ns,
                            monotonic_ns: 1,
                        },
                        class: session::ObservationClass::Connected,
                        tag: None,
                        attempts: None,
                        loss_count: None,
                    },
                )
                .unwrap();
            up_owner
                .set_kind(&mut turn, session::WorkKind::InFlightObservation)
                .unwrap();
            sink.persist_owned(
                &mut turn,
                &RecordFrame {
                    record_no: handle.prefix().next_record,
                    segment_no: handle.prefix().segment,
                    value: Record::Control(ControlRecord {
                        context: stamp(1).wire_context(config.active_context),
                        value: Control::Transport {
                            connection: a.connection_id,
                            epoch: a.tag.connection,
                            value: Transport::Up,
                        },
                    }),
                },
                gate,
                &up_owner,
            )
            .unwrap();
            handle
                .complete_observation(&mut turn, &sink, &up_owner, None)
                .unwrap();
            drop(up_owner);
            config.next_record_no = handle.prefix().next_record;
            let mut supervisor = PublicWsSupervisor::new(config, handle).unwrap();
            let start = supervisor.start_commands(&mut turn);
            start.outcome.unwrap();
            for command in start.commands {
                assert!(matches!(
                    authority.dispatch(&mut turn, command, |_| Ok::<(), ()>(())),
                    session::DispatchReport::Dispatched
                ));
            }
            assert_eq!(
                supervisor.snapshot(a.id).unwrap().transport,
                Transport::Unknown
            );
            let earlier = supervisor
                .handle
                .reserve_work(&mut turn, session::WorkKind::QueuedObservation)
                .unwrap();
            let attempt = CaptureAttemptNo::new(1).unwrap();
            supervisor
                .handle
                .admit_observation(
                    &mut turn,
                    &earlier,
                    session::ObservationIdentity {
                        stream: a.id,
                        epoch: a.tag.connection,
                        stamp: session::ReceiveStamp {
                            unix_ns: stamp(2).unix_ns,
                            monotonic_ns: 2,
                        },
                        class: session::ObservationClass::Raw,
                        tag: Some(a.tag),
                        attempts: Some((attempt, attempt)),
                        loss_count: None,
                    },
                )
                .unwrap();
            let due = 1 + HEARTBEAT_INTERVAL_NS;
            let admitted = supervisor.queue_tick(&mut turn, stamp(due));
            assert_eq!(admitted.outcome, Ok(AdmissionOutcome::Admitted));
            assert_eq!(admitted.admitted_scopes, [Some(a.id), None, None, None]);
            assert_eq!(
                supervisor.queued_items(),
                1,
                "authority Up schedules Timer even while the core transport is Unknown"
            );
            if with_cut {
                let b = &bindings[1];
                let failure = supervisor
                    .handle
                    .terminate(
                        &mut turn,
                        session::TerminalFailure {
                            stream: b.id,
                            connection: b.connection_id,
                            current_epoch: b.tag.connection,
                            observed_tag: b.tag,
                            context: supervisor.core.active_context,
                            stamp: session::ReceiveStamp {
                                unix_ns: stamp(due + 1).unix_ns,
                                monotonic_ns: due + 1,
                            },
                            input_class: session::InputClass::Connected,
                            attempt: session::AttemptIdentity::NotRaw,
                            cause: session::FailureCause::QueueOverflow,
                        },
                    )
                    .unwrap();
                drop(failure.close);
            }
            supervisor.synchronize_authority(&mut turn).unwrap();
            let ownership = supervisor.handle.authority().ownership_report();
            let cut = supervisor.core.cut_remaining;
            let queued = supervisor
                .core
                .queue
                .front()
                .unwrap()
                .observation_identity();
            let queued_owner_id = supervisor.queued_owners.front().unwrap().id();
            let status = supervisor.session_status();
            let close = supervisor.handle.authority().outstanding_close_owners();
            let prefix = supervisor.handle.prefix();
            let accepted_frames = frames.borrow().clone();
            for _ in 0..3 {
                let blocked = supervisor.drain_one(&mut turn, &mut sink);
                assert!(matches!(blocked.outcome,
                    Err(SupervisorError::Authority(session::AuthorityError::TimerOrderBlocked {
                        earlier_work_id,
                    })) if earlier_work_id == earlier.id()));
                assert_eq!(
                    supervisor
                        .core
                        .queue
                        .front()
                        .unwrap()
                        .observation_identity(),
                    queued
                );
                assert_eq!(
                    supervisor.queued_owners.front().unwrap().id(),
                    queued_owner_id
                );
                assert_eq!(supervisor.handle.authority().ownership_report(), ownership);
                assert_eq!(supervisor.core.cut_remaining, cut);
                assert_eq!(supervisor.session_status(), status);
                assert_eq!(
                    supervisor.handle.authority().outstanding_close_owners(),
                    close
                );
                assert_eq!(supervisor.handle.prefix(), prefix);
                assert_eq!(*frames.borrow(), accepted_frames);
                assert!(!supervisor.is_halted());
            }
            earlier
                .set_kind(&mut turn, session::WorkKind::InFlightObservation)
                .unwrap();
            sink.persist_owned(
                &mut turn,
                &RecordFrame {
                    record_no: supervisor.handle.prefix().next_record,
                    segment_no: supervisor.handle.prefix().segment,
                    value: Record::RawInput(RawInput {
                        context: stamp(2).wire_context(supervisor.core.active_context),
                        stream: a.id,
                        tag: a.tag,
                        attempt,
                        bytes: b"original".to_vec(),
                    }),
                },
                gate,
                &earlier,
            )
            .unwrap();
            supervisor
                .handle
                .complete_observation(&mut turn, &sink, &earlier, None)
                .unwrap();
            drop(earlier);
            // The external boundary write was not emitted by the private core.
            supervisor.core.next_record_no = Some(supervisor.handle.prefix().next_record);
            let mut result = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .unwrap()
                .unwrap();
            assert_eq!(result.records.len(), 1);
            assert_eq!(result.commands.len(), 1);
            let ping = std::mem::take(&mut result.commands)
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(ping.work_owner_id(), Some(queued_owner_id));
            assert!(matches!(
                authority.dispatch(&mut turn, ping, |_| Ok::<(), ()>(())),
                session::DispatchReport::Dispatched
            ));
            drop(result);
            assert_eq!(
                supervisor.handle.authority().ownership_report().work_used,
                0
            );
            assert_eq!(
                supervisor
                    .handle
                    .authority()
                    .ownership_report()
                    .pending_observations,
                0
            );
        }
    }

    #[test]
    fn timer_a_supervisor_authority_schedule_and_order_retry_durable() {
        timer_a_authority_scheduler_order_retry(RecordingGate::Durable);
    }

    #[test]
    fn timer_a_supervisor_authority_schedule_and_order_retry_written() {
        timer_a_authority_scheduler_order_retry(RecordingGate::Written);
    }

    fn drain_boundary(
        supervisor: &mut PublicWsSupervisor,
        owner: &mut recording::CaptureSessionOwner,
        turn: &mut session::SessionTurn,
        sink: &mut session::BoundRecordSink,
    ) -> Option<DrainResult> {
        let mut result = supervisor
            .drain_one(turn, sink)
            .outcome
            .expect("gated drain");
        if let Some(result) = result.as_mut() {
            dispatch_boundary(owner, turn, std::mem::take(&mut result.commands));
        }
        result
    }

    #[test]
    fn checked_generated_cancellation_error_retains_plan_owner_close_and_rightful_retry() {
        let policy = QueuePolicy {
            max_total_items: 9,
            ..QueuePolicy::default()
        };
        let (mut supervisor, mut owner, mut turn, mut sink, bindings, _) =
            bound_boundary_fixture_with_policy(policy);
        let a = &bindings[0];
        let started = supervisor.start_commands(&mut turn);
        started.outcome.expect("start");
        dispatch_boundary(&mut owner, &mut turn, started.commands);
        supervisor
            .queue_connected(&mut turn, a.connection_id, a.tag.connection, stamp(1))
            .outcome
            .expect("admitted Up");
        drop(drain_boundary(
            &mut supervisor,
            &mut owner,
            &mut turn,
            &mut sink,
        ));
        supervisor
            .queue_disconnected(&mut turn, a.connection_id, a.tag.connection, stamp(2))
            .outcome
            .expect("admitted Down");
        let mut down = supervisor
            .drain_one(&mut turn, &mut sink)
            .outcome
            .expect("actual gate-confirmed Down")
            .expect("Down result");
        let close = std::mem::take(&mut down.commands)
            .into_iter()
            .next()
            .expect("held mandatory Close");
        let close_owner = close.close_owner().expect("Close identity").clone();
        let work = supervisor
            .pending_owners
            .get(&a.id)
            .expect("generated Down owner");
        let work_id = work.id();
        // Private invariant negative: classification changes never settle the
        // genuine generated obligation. Force the checked cancellation error.
        work.set_kind(&mut turn, session::WorkKind::Command)
            .expect("rightful negative classification");
        for n in 0..5 {
            supervisor
                .queue_connected(&mut turn, a.connection_id, a.tag.connection, stamp(3 + n))
                .outcome
                .expect("admitted barrier");
        }
        let prefix = supervisor.handle.prefix();
        let before = supervisor.retention_report().ownership;
        let failed = supervisor.queue_text(
            &mut turn,
            a.connection_id,
            a.tag.connection,
            stamp(10),
            b"missing",
        );
        assert_eq!(
            failed.outcome,
            Err(SupervisorError::Authority(
                session::AuthorityError::InvalidOwner
            ))
        );
        assert!(!failed.cancelled_plan);
        assert_eq!(failed.close_owner, Some(close_owner.clone()));
        assert!(
            failed.commands.is_empty(),
            "existing lease is not duplicated"
        );
        assert_eq!(
            supervisor
                .pending_owners
                .get(&a.id)
                .expect("retained owner")
                .id(),
            work_id
        );
        assert!(supervisor.core.streams[&a.id].pending_disconnect.is_some());
        assert_eq!(supervisor.handle.prefix(), prefix);
        let status = supervisor.session_status();
        assert!(status.failed);
        assert_eq!(status.storage_stopped, None);
        assert_eq!(status.first_abandonment, None);
        let retained = supervisor.retention_report().ownership;
        assert_eq!(
            (
                retained.work_used,
                retained.work_references,
                retained.pending_observations
            ),
            (
                before.work_used,
                before.work_references,
                before.pending_observations
            )
        );

        let (_, _foreign_owner, mut foreign_turn, _, _, _) = bound_boundary_fixture();
        assert_eq!(
            supervisor.cancel_pending_disconnect(&mut foreign_turn, a.id),
            Err(SupervisorError::Authority(
                session::AuthorityError::AuthorityMismatch
            ))
        );
        assert_eq!(supervisor.session_status(), status);
        assert_eq!(supervisor.handle.prefix(), prefix);
        assert_eq!(
            supervisor
                .pending_owners
                .get(&a.id)
                .expect("rightful pending owner")
                .id(),
            work_id
        );

        assert_eq!(
            owner
                .close_diagnostic(&mut turn)
                .outcome
                .expect("poll diagnostic close"),
            recording::DiagnosticCloseState::Closing
        );
        let closing = supervisor.session_status();
        assert_eq!(
            supervisor.drain_one(&mut turn, &mut sink).outcome.err(),
            Some(SupervisorError::Authority(
                session::AuthorityError::InvalidOwner
            ))
        );
        assert_eq!(supervisor.session_status(), closing);
        assert_eq!(supervisor.handle.prefix(), prefix);
        assert_eq!(
            supervisor
                .pending_owners
                .get(&a.id)
                .expect("failed cancellation still owned")
                .id(),
            work_id
        );
        assert!(supervisor.core.streams[&a.id].pending_disconnect.is_some());

        supervisor
            .pending_owners
            .get(&a.id)
            .expect("same owner for retry")
            .set_kind(&mut turn, session::WorkKind::PendingPlan)
            .expect("restore classification");
        supervisor
            .synchronize_authority(&mut turn)
            .expect("same authoritative plan settles");
        assert!(!supervisor.pending_owners.contains_key(&a.id));
        assert!(supervisor.core.streams[&a.id].pending_disconnect.is_none());
        assert_eq!(supervisor.handle.prefix(), prefix);
        assert_eq!(
            supervisor.session_status().first_failure,
            status.first_failure
        );
        assert_eq!(
            supervisor.session_status().cut_sequence,
            status.cut_sequence
        );
        assert_eq!(supervisor.retention_report().ownership.abandoned_work, 0);
        assert!(matches!(
            owner.dispatch(&mut turn, close, |_| Ok::<_, ()>(())),
            session::DispatchReport::Dispatched
        ));
        drop(down);
        for _ in 0..5 {
            drop(drain_boundary(
                &mut supervisor,
                &mut owner,
                &mut turn,
                &mut sink,
            ));
        }
        let marker = drain_boundary(&mut supervisor, &mut owner, &mut turn, &mut sink)
            .expect("failure marker");
        assert_eq!(marker.records.len(), 1);
        drop(marker);
        assert_eq!(supervisor.retention_report().ownership.work_used, 0);
        assert_eq!(supervisor.session_status().storage_stopped, None);
        assert_eq!(supervisor.session_status().first_abandonment, None);
        assert_eq!(
            owner
                .close_diagnostic(&mut turn)
                .outcome
                .expect("complete diagnostic close"),
            recording::DiagnosticCloseState::Closed
        );
    }

    #[test]
    fn canonical_capture_attempt_exhaustion_terminates_scope_and_preserves_neighbor_diagnostic_service()
     {
        for old_tag in [false, true] {
            let (mut supervisor, mut owner, mut turn, mut sink, bindings, path) =
                bound_boundary_fixture();
            let a = &bindings[0];
            let b = &bindings[1];
            let started = supervisor.start_commands(&mut turn);
            started.outcome.expect("start");
            dispatch_boundary(&mut owner, &mut turn, started.commands);
            for binding in &bindings {
                supervisor
                    .queue_connected(
                        &mut turn,
                        binding.connection_id,
                        binding.tag.connection,
                        stamp(0),
                    )
                    .outcome
                    .expect("connected");
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
            }
            if old_tag {
                supervisor
                    .queue_disconnected(&mut turn, a.connection_id, a.tag.connection, stamp(1))
                    .outcome
                    .expect("disconnect");
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
                let epoch = supervisor.snapshot(a.id).expect("new tag").tag.connection;
                supervisor
                    .queue_connected(
                        &mut turn,
                        a.connection_id,
                        epoch,
                        stamp(RECONNECT_MAX_NS_V1 + 10),
                    )
                    .outcome
                    .expect("reconnected");
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
            }
            let tag = supervisor.snapshot(a.id).expect("tag").tag;
            // Synthetic but WAL-valid earlier accounting compresses the
            // counter-boundary fixture; no MAX+1, raw fabrication or writer
            // export is used to establish its accepted prefix.
            let gap_owner = supervisor
                .handle
                .reserve_work(&mut turn, session::WorkKind::InFlightObservation)
                .expect("counted seed");
            let gap_no = supervisor.handle.prefix().next_record;
            let seed = RecordFrame {
                record_no: gap_no,
                segment_no: SegmentNo::new(0),
                value: Record::Gap(Gap {
                    context: stamp(RECONNECT_MAX_NS_V1 + 11)
                        .wire_context(supervisor.core.active_context),
                    scope: GapScope::ExplicitTargets(vec![GapTarget {
                        stream: a.id,
                        tag,
                        range: Some((
                            CaptureAttemptNo::new(1).expect("first"),
                            CaptureAttemptNo::new(u64::MAX - 1).expect("last"),
                        )),
                        loss_count: Some(u64::MAX - 1),
                    }]),
                    reason: Reason::QueueOverflow,
                }),
            };
            sink.persist_owned(&mut turn, &seed, RecordingGate::Durable, &gap_owner)
                .expect("truthful seed receipt");
            drop(gap_owner);
            supervisor.core.next_record_no = Some(supervisor.handle.prefix().next_record);
            let runtime = supervisor.core.streams.get_mut(&a.id).expect("scope");
            runtime.capture_attempt_frontier = u64::MAX - 1;
            runtime.accounted_attempt_frontier = u64::MAX - 1;
            supervisor
                .queue_text(
                    &mut turn,
                    a.connection_id,
                    tag.connection,
                    stamp(RECONNECT_MAX_NS_V1 + 20),
                    b"{",
                )
                .outcome
                .expect("MAX admitted");
            let observed = if old_tag {
                a.tag.connection
            } else {
                tag.connection
            };
            let failed = supervisor.queue_text(
                &mut turn,
                a.connection_id,
                observed,
                stamp(RECONNECT_MAX_NS_V1 + 21),
                b"unrepresented",
            );
            assert_eq!(
                failed.outcome,
                Err(SupervisorError::CounterExhausted("CaptureAttemptNo"))
            );
            let failure = failed.failure.expect("reserved exact failure");
            assert_eq!(
                failure.attempt,
                session::AttemptIdentity::NoRepresentableSuccessor { frontier: u64::MAX }
            );
            assert_eq!(failure.current_epoch, tag.connection);
            assert_eq!(failure.observed_tag.connection, observed);
            assert_eq!(failure.stamp.monotonic_ns, RECONNECT_MAX_NS_V1 + 21);
            assert!(matches!(
                failed.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
            dispatch_boundary(&mut owner, &mut turn, failed.commands);
            let count = supervisor.retention_report().ownership.work_used;
            for _ in 0..3 {
                assert_eq!(
                    supervisor
                        .queue_text(
                            &mut turn,
                            a.connection_id,
                            observed,
                            stamp(u64::MAX),
                            b"unrepresented"
                        )
                        .outcome,
                    Ok(AdmissionOutcome::AlreadyTerminated)
                );
                assert_eq!(
                    supervisor
                        .queue_text(
                            &mut turn,
                            a.connection_id,
                            tag.connection,
                            stamp(u64::MAX),
                            b"pong"
                        )
                        .outcome,
                    Ok(AdmissionOutcome::AlreadyTerminated)
                );
                assert_eq!(
                    supervisor
                        .queue_connected(
                            &mut turn,
                            a.connection_id,
                            tag.connection.checked_next().expect("next"),
                            stamp(u64::MAX)
                        )
                        .outcome,
                    Ok(AdmissionOutcome::AlreadyTerminated)
                );
                assert_eq!(
                    supervisor
                        .queue_disconnected(
                            &mut turn,
                            a.connection_id,
                            tag.connection,
                            stamp(u64::MAX)
                        )
                        .outcome,
                    Ok(AdmissionOutcome::AlreadyTerminated)
                );
                assert_eq!(supervisor.terminal_failure(a.id), Some(failure));
                assert_eq!(supervisor.retention_report().ownership.work_used, count);
            }
            assert_eq!(
                supervisor
                    .snapshot(a.id)
                    .expect("frontier")
                    .capture_attempt_frontier,
                u64::MAX
            );
            assert_eq!(
                supervisor
                    .snapshot(a.id)
                    .expect("frontier")
                    .accounted_attempt_frontier,
                u64::MAX - 1
            );
            let report = supervisor.drain_one(&mut turn, &mut sink);
            assert!(matches!(
                report.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
            let result = report
                .outcome
                .expect("MAX diagnostic drain")
                .expect("MAX result");
            assert_eq!(result.records.len(), 1);
            drop(result);
            assert_eq!(
                supervisor
                    .snapshot(a.id)
                    .expect("accounted")
                    .accounted_attempt_frontier,
                u64::MAX
            );
            drop(drain_boundary(
                &mut supervisor,
                &mut owner,
                &mut turn,
                &mut sink,
            ));
            supervisor.queue_text(&mut turn, b.connection_id, b.tag.connection, stamp(RECONNECT_MAX_NS_V1 + 22), br#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books50","symbol":"ETHUSDT"}}"#).outcome.expect("neighbor raw");
            drop(drain_boundary(
                &mut supervisor,
                &mut owner,
                &mut turn,
                &mut sink,
            ));
            supervisor
                .queue_tick(&mut turn, stamp(RECONNECT_MAX_NS_V1 + 23))
                .outcome
                .expect("neighbor ping proposal");
            let report = supervisor.drain_one(&mut turn, &mut sink);
            let mut result = report
                .outcome
                .expect("neighbor timer")
                .expect("neighbor result");
            assert!(result.commands.iter().any(|command| command.connection() == b.connection_id && matches!(command.kind(), session::CommandKind::SendText { text } if text == "ping")));
            dispatch_boundary(&mut owner, &mut turn, std::mem::take(&mut result.commands));
            drop(result);
            assert!(!supervisor.is_halted());
            assert!(owner.begin_finalization(&mut turn).is_err());
            drop(sink);
            drop(owner);
            let mut reader = recording::WalReader::open(&path).expect("reader");
            while reader
                .next_record()
                .expect("valid bounded prefix")
                .is_some()
            {}
            assert_eq!(
                reader.report().status,
                recording::ArchiveStatus::ValidPrefixIncomplete
            );
            assert_eq!(reader.report().input_quality, None);
            drop(reader);
        }
    }

    #[test]
    fn capture_termination_drains_admitted_connected_and_pong_without_live_revival() {
        let policy = QueuePolicy {
            max_total_items: 9,
            ..QueuePolicy::default()
        };
        let (mut supervisor, mut owner, mut turn, mut sink, bindings, path) =
            bound_boundary_fixture_with_policy(policy);
        let a = &bindings[0];
        let b = &bindings[1];
        let started = supervisor.start_commands(&mut turn);
        started.outcome.expect("start");
        dispatch_boundary(&mut owner, &mut turn, started.commands);
        supervisor
            .queue_connected(&mut turn, a.connection_id, a.tag.connection, stamp(10))
            .outcome
            .expect("precut Connected A");
        supervisor
            .queue_text(
                &mut turn,
                a.connection_id,
                a.tag.connection,
                stamp(11),
                b"pong",
            )
            .outcome
            .expect("precut Pong A");
        supervisor
            .queue_connected(&mut turn, b.connection_id, b.tag.connection, stamp(12))
            .outcome
            .expect("precut Connected B");
        supervisor
            .queue_text(
                &mut turn,
                b.connection_id,
                b.tag.connection,
                stamp(13),
                b"pong",
            )
            .outcome
            .expect("precut Pong B");
        supervisor
            .queue_text(
                &mut turn,
                a.connection_id,
                a.tag.connection,
                stamp(14),
                b"{",
            )
            .outcome
            .expect("precut Raw A1");
        supervisor
            .queue_text(
                &mut turn,
                a.connection_id,
                a.tag.connection,
                stamp(15),
                b"pong",
            )
            .outcome
            .expect("precut later Pong A");
        let failed = supervisor.queue_text(
            &mut turn,
            a.connection_id,
            a.tag.connection,
            stamp(16),
            b"{",
        );
        assert_eq!(
            failed.outcome,
            Err(SupervisorError::QueueExhausted { stream: a.id })
        );
        dispatch_boundary(&mut owner, &mut turn, failed.commands);
        let before = supervisor.snapshot(a.id).expect("terminated scope");
        assert!(before.capture_terminated);
        assert_eq!(before.transport, Transport::Unknown);
        for index in 0..6 {
            let report = supervisor.drain_one(&mut turn, &mut sink);
            assert!(matches!(
                report.session_disposition,
                session::SessionDisposition::DiagnosticOnly { .. }
            ));
            let mut result = report
                .outcome
                .expect("historical gated drain")
                .expect("record");
            assert_eq!(result.records.len(), 1);
            if [0, 1, 5].contains(&index) {
                assert!(result.commands.is_empty());
            }
            dispatch_boundary(&mut owner, &mut turn, std::mem::take(&mut result.commands));
            assert_eq!(
                supervisor
                    .snapshot(a.id)
                    .expect("no capture revival")
                    .transport,
                Transport::Unknown
            );
        }
        drop(drain_boundary(
            &mut supervisor,
            &mut owner,
            &mut turn,
            &mut sink,
        ));
        let mut expected = before;
        expected.accounted_attempt_frontier = 1;
        expected.queued_raw_frames = 0;
        expected.queued_raw_bytes = 0;
        assert_eq!(supervisor.snapshot(a.id).expect("same failure"), expected);
        drop(sink);
        drop(owner);
        let mut reader = recording::WalReader::open(&path).expect("reader");
        let mut observed = Vec::new();
        while let Some(frame) = reader.next_record().expect("accepted prefix") {
            if let Record::Control(ControlRecord {
                context,
                value:
                    Control::Transport {
                        connection,
                        value: Transport::Up,
                        ..
                    },
            }) = frame.value
            {
                observed.push((connection, context.monotonic_ns.get()));
            }
        }
        assert_eq!(
            observed,
            [
                (a.connection_id, 10),
                (a.connection_id, 11),
                (b.connection_id, 12),
                (b.connection_id, 13),
                (a.connection_id, 15)
            ]
        );
        drop(reader);
    }
    fn external_failed_marker(
        supervisor: &PublicWsSupervisor,
        at: u64,
        reason: Reason,
    ) -> RecordFrame {
        RecordFrame {
            record_no: supervisor.handle.prefix().next_record,
            segment_no: supervisor.core.segment_no,
            value: Record::Control(ControlRecord {
                context: stamp(at).wire_context(supervisor.core.active_context),
                value: Control::Recording(domain::record::RecordingEvidence {
                    health: domain::record::RecordingHealth::Failed,
                    kind: WatermarkKind::Durable,
                    through: supervisor
                        .handle
                        .authority()
                        .trusted_watermark(WatermarkKind::Durable),
                    reason,
                }),
            }),
        }
    }

    fn boundary_queue_gap(
        supervisor: &PublicWsSupervisor,
        stream: StreamId,
    ) -> (u64, u64, u64, ReceiveStamp) {
        supervisor
            .core
            .queue
            .iter()
            .find_map(|ingress| match ingress {
                Ingress::QueueGap {
                    stream: stored,
                    first_attempt,
                    last_attempt,
                    loss_count,
                    stamp,
                    ..
                } if *stored == stream => {
                    Some((first_attempt.get(), last_attempt.get(), *loss_count, *stamp))
                }
                _ => None,
            })
            .expect("exact admitted GAP")
    }

    #[test]
    fn external_archive_failure_freezes_pre_cut_gap_and_preserves_first_marker_descriptor() {
        for free_work in [false, true] {
            let policy = QueuePolicy {
                max_total_items: 9,
                ..QueuePolicy::default()
            };
            let (mut supervisor, mut owner, mut turn, mut sink, bindings, path) =
                bound_boundary_fixture_with_policy(policy);
            let a = &bindings[0];
            let b = &bindings[1];
            let started = supervisor.start_commands(&mut turn);
            started.outcome.expect("start");
            dispatch_boundary(&mut owner, &mut turn, started.commands);
            for binding in &bindings {
                supervisor
                    .queue_connected(
                        &mut turn,
                        binding.connection_id,
                        binding.tag.connection,
                        stamp(0),
                    )
                    .outcome
                    .expect("connected");
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
            }
            supervisor.queue_text(&mut turn, b.connection_id, b.tag.connection, stamp(1), br#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books50","symbol":"ETHUSDT"}}"#).outcome.expect("B Raw1");
            drop(drain_boundary(
                &mut supervisor,
                &mut owner,
                &mut turn,
                &mut sink,
            ));
            for at in 10..15 {
                supervisor
                    .queue_text(
                        &mut turn,
                        a.connection_id,
                        a.tag.connection,
                        stamp(at),
                        b"pong",
                    )
                    .outcome
                    .expect("five distinct earlier owners");
            }
            let too_large = vec![b'x'; policy.max_raw_message_bytes + 1];
            supervisor
                .queue_text(
                    &mut turn,
                    b.connection_id,
                    b.tag.connection,
                    stamp(20),
                    &too_large,
                )
                .outcome
                .expect("B Gap2");
            supervisor
                .queue_text(
                    &mut turn,
                    b.connection_id,
                    b.tag.connection,
                    stamp(21),
                    &too_large,
                )
                .outcome
                .expect("B Gap2..3");
            assert_eq!(supervisor.retention_report().ownership.work_used, 6);
            let frozen = boundary_queue_gap(&supervisor, b.id);
            assert_eq!(frozen, (2, 3, 2, stamp(20)));
            let external = external_failed_marker(&supervisor, 25, Reason::Unknown);
            assert_eq!(
                sink.persist_marker(&mut turn, &external, RecordingGate::Durable),
                Err(session::PersistBoundaryError::Authority(
                    session::AuthorityError::NotQuiescent
                ))
            );
            assert!(supervisor.session_status().failed);
            assert_eq!(supervisor.session_status().storage_stopped, None);
            assert_eq!(supervisor.session_status().first_failure, None);
            assert_eq!(supervisor.retention_report().cut_remaining, Some(6));
            assert_eq!(supervisor.retention_report().ownership.pre_cut, 6);
            if free_work {
                drop(drain_boundary(
                    &mut supervisor,
                    &mut owner,
                    &mut turn,
                    &mut sink,
                ));
            }
            let post = supervisor.queue_text(
                &mut turn,
                b.connection_id,
                b.tag.connection,
                stamp(40),
                &too_large,
            );
            if free_work {
                post.outcome.expect("separate counted PostCut GAP4");
                assert!(post.failure.is_none());
                assert_eq!(supervisor.retention_report().ownership.pre_cut, 5);
                assert_eq!(supervisor.retention_report().ownership.post_cut, 1);
            } else {
                assert_eq!(
                    post.outcome,
                    Err(SupervisorError::QueueExhausted { stream: b.id })
                );
                let failure = post.failure.expect("exact B terminal");
                assert_eq!(
                    failure.attempt,
                    session::AttemptIdentity::Candidate(
                        CaptureAttemptNo::new(4).expect("candidate")
                    )
                );
                assert_eq!(failure.stamp.monotonic_ns, 40);
                dispatch_boundary(&mut owner, &mut turn, post.commands);
            }
            assert_eq!(boundary_queue_gap(&supervisor, b.id), frozen);
            assert_eq!(supervisor.retention_report().ownership.work_used, 6);
            assert_eq!(
                supervisor
                    .session_status()
                    .archive_observation
                    .expect("first archive descriptor")
                    .context
                    .monotonic_ns
                    .get(),
                25
            );
            assert_eq!(
                supervisor
                    .session_status()
                    .archive_observation
                    .expect("first archive reason")
                    .reason,
                Reason::Unknown
            );
            while let Some(result) =
                drain_boundary(&mut supervisor, &mut owner, &mut turn, &mut sink)
            {
                drop(result);
            }
            assert_eq!(
                supervisor
                    .snapshot(b.id)
                    .expect("accounted B")
                    .accounted_attempt_frontier,
                if free_work { 4 } else { 3 }
            );
            drop(sink);
            drop(owner);
            let mut reader = recording::WalReader::open(&path).expect("reader");
            let mut evidence = Vec::new();
            let mut marker_no = None;
            let mut gaps = Vec::new();
            while let Some(frame) = reader.next_record().expect("valid bounded prefix") {
                match frame.value {
                    Record::Control(ControlRecord {
                        context,
                        value: Control::Recording(recording),
                    }) => {
                        marker_no = Some(frame.record_no);
                        evidence.push((context.monotonic_ns.get(), recording));
                    }
                    Record::Gap(gap) if gap.reason == Reason::QueueOverflow => {
                        if let GapScope::ExplicitTargets(targets) = gap.scope
                            && let Some(target) = targets.first()
                            && target.stream == b.id
                        {
                            gaps.push((
                                frame.record_no,
                                target.range.expect("range"),
                                target.loss_count,
                            ));
                        }
                    }
                    _ => {}
                }
            }
            assert_eq!(evidence.len(), 1);
            assert_eq!(evidence[0].0, 25);
            assert_eq!(evidence[0].1.reason, Reason::Unknown);
            let marker_no = marker_no.expect("reserved archive marker");
            assert!(
                evidence[0]
                    .1
                    .through
                    .is_some_and(|through| through < marker_no)
            );
            assert_eq!(gaps.len(), if free_work { 2 } else { 1 });
            assert!(gaps[0].0 < marker_no);
            assert_eq!(
                (gaps[0].1.0.get(), gaps[0].1.1.get(), gaps[0].2),
                (2, 3, Some(2))
            );
            if free_work {
                assert!(gaps[1].0 > marker_no);
                assert_eq!(
                    (gaps[1].1.0.get(), gaps[1].1.1.get(), gaps[1].2),
                    (4, 4, Some(1))
                );
            }
        }
    }

    #[test]
    fn successful_external_marker_is_authoritative_without_acknowledgement_or_duplicate() {
        let (mut supervisor, mut owner, mut turn, mut sink, bindings, path) =
            bound_boundary_fixture();
        let a = &bindings[0];
        let started = supervisor.start_commands(&mut turn);
        started.outcome.expect("start");
        let marker = external_failed_marker(&supervisor, 100, Reason::Unknown);
        let receipt = sink
            .persist_marker(&mut turn, &marker, RecordingGate::Durable)
            .expect("gated external marker");
        assert_eq!(
            supervisor.session_status().marker,
            session::MarkerState::Confirmed(receipt.through)
        );
        assert_eq!(supervisor.retention_report().cut_remaining, Some(0));
        assert_eq!(
            supervisor.retention_report().ownership.work_used,
            2,
            "held Connect leases stay counted"
        );
        dispatch_boundary(&mut owner, &mut turn, started.commands);
        let queued =
            supervisor.queue_connected(&mut turn, a.connection_id, a.tag.connection, stamp(101));
        assert!(matches!(
            queued.session_disposition,
            session::SessionDisposition::DiagnosticOnly { .. }
        ));
        queued.outcome.expect("post-cut neighbor transport");
        let result = drain_boundary(&mut supervisor, &mut owner, &mut turn, &mut sink)
            .expect("post-cut record");
        assert_eq!(
            result.records[0],
            receipt
                .through
                .checked_next()
                .expect("authenticated successor")
        );
        drop(result);
        assert!(
            supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("empty")
                .is_none()
        );
        assert_eq!(
            supervisor
                .snapshot(a.id)
                .expect("attempt unchanged")
                .capture_attempt_frontier,
            0
        );
        drop(sink);
        drop(owner);
        let mut reader = recording::WalReader::open(&path).expect("reader");
        let mut failed = 0;
        while let Some(frame) = reader.next_record().expect("valid prefix") {
            if matches!(
                frame.value,
                Record::Control(ControlRecord {
                    value: Control::Recording(domain::record::RecordingEvidence {
                        health: domain::record::RecordingHealth::Failed,
                        ..
                    }),
                    ..
                })
            ) {
                failed += 1;
            }
        }
        assert_eq!(
            failed, 1,
            "actual external receipt prevents a duplicate marker"
        );
    }

    #[test]
    fn constructor_rejects_unused_handles_after_closing_failure_and_finalization() {
        for state in 0..3 {
            let (config, handle, mut owner, mut turn, mut sink, _, _) =
                unconstructed_boundary_fixture(QueuePolicy::default());
            let marker = RecordFrame {
                record_no: handle.prefix().next_record,
                segment_no: config.segment_no,
                value: Record::Control(ControlRecord {
                    context: stamp(100).wire_context(config.active_context),
                    value: Control::Recording(domain::record::RecordingEvidence {
                        health: domain::record::RecordingHealth::Failed,
                        kind: WatermarkKind::Durable,
                        through: handle.trusted_watermark(WatermarkKind::Durable),
                        reason: Reason::Unknown,
                    }),
                }),
            };
            let expected = match state {
                0 => {
                    let _ticket = owner.begin_finalization(&mut turn).expect("Closing");
                    session::AuthorityError::SessionClosing
                }
                1 => {
                    sink.persist_marker(&mut turn, &marker, RecordingGate::Durable)
                        .expect("archive failed");
                    session::AuthorityError::ArchiveFailed
                }
                _ => {
                    let ticket = owner.begin_finalization(&mut turn).expect("Closing");
                    let session::QuiescenceReport::Ready(mut proof) =
                        handle.quiesce(&mut turn, &ticket)
                    else {
                        panic!("empty registered ownership");
                    };
                    owner
                        .finalize(&mut turn, &mut proof)
                        .expect("explicit healthy finalization");
                    session::AuthorityError::SessionClosed
                }
            };
            assert!(
                matches!(PublicWsSupervisor::new(config, handle), Err(SupervisorError::Authority(error)) if error == expected)
            );
        }
    }

    #[test]
    fn external_scope_termination_suppresses_revival_but_keeps_admitted_historical_up() {
        let (mut supervisor, mut owner, mut turn, mut sink, bindings, _) = bound_boundary_fixture();
        let a = &bindings[0];
        let started = supervisor.start_commands(&mut turn);
        started.outcome.expect("start");
        dispatch_boundary(&mut owner, &mut turn, started.commands);
        supervisor
            .queue_connected(&mut turn, a.connection_id, a.tag.connection, stamp(10))
            .outcome
            .expect("admitted Connected");
        let failure = session::TerminalFailure {
            stream: a.id,
            connection: a.connection_id,
            observed_tag: a.tag,
            current_epoch: a.tag.connection,
            context: supervisor.core.active_context,
            stamp: session::ReceiveStamp {
                unix_ns: stamp(11).unix_ns,
                monotonic_ns: 11,
            },
            input_class: session::InputClass::Raw,
            attempt: session::AttemptIdentity::Candidate(
                CaptureAttemptNo::new(1).expect("candidate"),
            ),
            cause: session::FailureCause::QueueOverflow,
        };
        let terminated = sink
            .authority()
            .terminate(&mut turn, failure)
            .expect("same-owner terminal decision");
        // Read-only reporting must expose the reserved slot before any
        // mutable supervisor entry has synchronized the core runtime.
        assert_eq!(supervisor.retention_report().filled_terminal_slots, 1);
        let external_snapshot = supervisor.snapshot(a.id).expect("configured stream");
        assert!(external_snapshot.capture_terminated);
        assert_eq!(external_snapshot.capture_attempt_frontier, 1);
        dispatch_boundary(
            &mut owner,
            &mut turn,
            terminated.close.map(|close| close.into_command().unwrap()),
        );
        let report = supervisor.drain_one(&mut turn, &mut sink);
        assert!(matches!(
            report.session_disposition,
            session::SessionDisposition::DiagnosticOnly { .. }
        ));
        let result = report
            .outcome
            .expect("admitted Connected drain")
            .expect("historical Up");
        assert_eq!(result.records.len(), 1);
        assert!(result.commands.is_empty());
        drop(result);
        let snapshot = supervisor.snapshot(a.id).expect("terminal");
        assert!(snapshot.capture_terminated);
        assert_eq!(snapshot.transport, Transport::Unknown);
        assert_eq!(snapshot.subscription, SubscriptionState::Degraded);
        assert_eq!(snapshot.capture_attempt_frontier, 1);
        assert_eq!(snapshot.accounted_attempt_frontier, 0);
        assert_eq!(
            supervisor
                .queue_text(
                    &mut turn,
                    a.connection_id,
                    a.tag.connection,
                    stamp(12),
                    b"ignored"
                )
                .outcome,
            Ok(AdmissionOutcome::AlreadyTerminated)
        );
        drop(drain_boundary(
            &mut supervisor,
            &mut owner,
            &mut turn,
            &mut sink,
        ));
    }
    #[test]
    fn start_capacity_rejection_preserves_admission_order_and_start_state() {
        let policy = QueuePolicy {
            max_total_items: 9,
            ..QueuePolicy::default()
        };
        let (mut supervisor, _owner, mut turn, _sink, bindings, _) =
            bound_boundary_fixture_with_policy(policy);
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(
                supervisor
                    .handle
                    .reserve_work(&mut turn, session::WorkKind::Command)
                    .expect("held counted jobs"),
            );
        }
        let last = held.last().expect("last held admission").id();
        let snapshots: Vec<_> = bindings
            .iter()
            .map(|binding| supervisor.snapshot(binding.id).expect("scope"))
            .collect();
        let rejected = supervisor.start_commands(&mut turn);
        assert_eq!(
            rejected.outcome,
            Err(SupervisorError::Authority(
                session::AuthorityError::WorkExhausted
            ))
        );
        assert!(rejected.commands.is_empty());
        assert!(!supervisor.core.started);
        assert_eq!(supervisor.retention_report().ownership.work_used, 5);
        for (binding, snapshot) in bindings.iter().zip(snapshots) {
            assert_eq!(
                supervisor.snapshot(binding.id).expect("unchanged"),
                snapshot
            );
        }
        drop(held.pop());
        let next = supervisor
            .handle
            .reserve_work(&mut turn, session::WorkKind::Command)
            .expect("next actual admission");
        assert_eq!(
            next.id(),
            last + 1,
            "capacity rejection creates no partial admission sequence"
        );
    }

    #[test]
    fn received_admission_order_exhaustion_uses_reserved_exact_failure_and_keeps_prefix_drainable()
    {
        // Domain's private MAX-sequence test establishes the real checked
        // reservation error. This private seam supplies that negative result
        // only; all ownership, Close dispatch and receipts use a genuine owner.
        for class in 0..5 {
            let (mut supervisor, mut owner, mut turn, mut sink, bindings, path) =
                bound_boundary_fixture();
            let a = &bindings[0];
            let b = &bindings[1];
            let started = supervisor.start_commands(&mut turn);
            started.outcome.expect("start");
            dispatch_boundary(&mut owner, &mut turn, started.commands);
            if class == 4 {
                let counted = supervisor
                    .handle
                    .reserve_work(&mut turn, session::WorkKind::InFlightObservation)
                    .expect("counted synthetic counter-boundary prefix");
                let gap = RecordFrame {
                    record_no: supervisor.handle.prefix().next_record,
                    segment_no: SegmentNo::new(0),
                    value: Record::Gap(Gap {
                        context: stamp(1).wire_context(supervisor.core.active_context),
                        scope: GapScope::ExplicitTargets(vec![GapTarget {
                            stream: a.id,
                            tag: a.tag,
                            range: Some((
                                CaptureAttemptNo::new(1).expect("first"),
                                CaptureAttemptNo::new(u64::MAX).expect("last"),
                            )),
                            loss_count: Some(u64::MAX),
                        }]),
                        reason: Reason::QueueOverflow,
                    }),
                };
                sink.persist_owned(&mut turn, &gap, RecordingGate::Durable, &counted)
                    .expect("actual counter-boundary prefix receipt");
                drop(counted);
                let runtime = supervisor.core.streams.get_mut(&a.id).expect("stream");
                runtime.capture_attempt_frontier = u64::MAX;
                runtime.accounted_attempt_frontier = u64::MAX;
            }
            for binding in &bindings {
                supervisor
                    .queue_connected(
                        &mut turn,
                        binding.connection_id,
                        binding.tag.connection,
                        stamp(10 + binding.id.get() as u64),
                    )
                    .outcome
                    .expect("admitted before failure");
            }
            let (call, expected_class, expected_attempt) = match class {
                0 => (
                    ReceivedCall::Text(b"unrepresented"),
                    session::InputClass::Raw,
                    session::AttemptIdentity::Candidate(
                        CaptureAttemptNo::new(1).expect("candidate"),
                    ),
                ),
                1 => (
                    ReceivedCall::Text(b"pong"),
                    session::InputClass::Pong,
                    session::AttemptIdentity::NotRaw,
                ),
                2 => (
                    ReceivedCall::Connected,
                    session::InputClass::Connected,
                    session::AttemptIdentity::NotRaw,
                ),
                3 => (
                    ReceivedCall::Disconnected,
                    session::InputClass::Disconnected,
                    session::AttemptIdentity::NotRaw,
                ),
                _ => (
                    ReceivedCall::Text(b"unrepresented"),
                    session::InputClass::Raw,
                    session::AttemptIdentity::NoRepresentableSuccessor { frontier: u64::MAX },
                ),
            };
            let mut reserve_calls = 0;
            let failed = supervisor.admit_with_reserver(
                &mut turn,
                Some(a.connection_id),
                Some(a.tag.connection),
                stamp(100),
                call,
                |_, _| {
                    reserve_calls += 1;
                    Err(session::AuthorityError::CounterExhausted("AdmissionOrder"))
                },
            );
            assert_eq!(reserve_calls, 1);
            assert_eq!(
                failed.outcome,
                Err(SupervisorError::Authority(
                    session::AuthorityError::CounterExhausted("AdmissionOrder")
                ))
            );
            let exact = failed.failure.expect("reserved exact failure");
            assert_eq!(exact.stream, a.id);
            assert_eq!(exact.observed_tag, a.tag);
            assert_eq!(exact.stamp.monotonic_ns, 100);
            assert_eq!(exact.stamp.unix_ns, stamp(100).unix_ns);
            assert_eq!(exact.input_class, expected_class);
            assert_eq!(exact.attempt, expected_attempt);
            assert_eq!(
                exact.cause,
                session::FailureCause::CounterExhausted("AdmissionOrder")
            );
            assert!(supervisor.session_status().failed);
            assert!(supervisor.session_status().storage_stopped.is_none());
            assert!(!supervisor.is_halted());
            let retained = supervisor.retention_report();
            assert_eq!(retained.ownership.work_used, 2);
            assert_eq!(retained.filled_terminal_slots, 1);
            assert_eq!(retained.cut_remaining, Some(2));
            dispatch_boundary(&mut owner, &mut turn, failed.commands);
            for _ in 0..3 {
                assert_eq!(
                    supervisor
                        .queue_text(
                            &mut turn,
                            a.connection_id,
                            a.tag.connection,
                            stamp(200),
                            b"retry"
                        )
                        .outcome,
                    Ok(AdmissionOutcome::AlreadyTerminated)
                );
                assert_eq!(supervisor.terminal_failure(a.id), Some(exact));
                assert_eq!(supervisor.retention_report().ownership.work_used, 2);
            }
            let a_up = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("diagnostic admitted Up")
                .expect("first admitted owner");
            assert!(a_up.commands.is_empty());
            drop(a_up);
            let b_up = supervisor
                .drain_one(&mut turn, &mut sink)
                .outcome
                .expect("neighbor admitted Up")
                .expect("second admitted owner");
            assert_eq!(b_up.commands.len(), 1);
            dispatch_boundary(&mut owner, &mut turn, b_up.commands);
            drop(drain_boundary(
                &mut supervisor,
                &mut owner,
                &mut turn,
                &mut sink,
            ));
            assert!(matches!(
                owner.begin_finalization(&mut turn),
                Err(recording::OwnerError::Authority(
                    session::AuthorityError::ArchiveFailed
                ))
            ));
            assert_eq!(
                supervisor.snapshot(b.id).expect("neighbor").transport,
                Transport::Up
            );
            let mut reader = recording::WalReader::open(&path).expect("actual prefix");
            let mut observed_marker = 0;
            while let Some(frame) = reader.next_record().expect("valid prefix") {
                assert!(!matches!(frame.value, Record::RawInput(_)));
                if let Record::Control(ControlRecord {
                    context,
                    value: Control::Recording(evidence),
                }) = frame.value
                {
                    observed_marker += 1;
                    assert_eq!(context.monotonic_ns.get(), 100);
                    assert_eq!(evidence.reason, Reason::Unknown);
                    assert!(
                        evidence
                            .through
                            .is_some_and(|earlier| earlier < frame.record_no)
                    );
                }
            }
            assert_eq!(observed_marker, 1);
        }
    }

    #[test]
    fn same_side_f2_loss_coalescing_does_not_reserve_new_admission_order() {
        for post_cut in [false, true] {
            let policy = QueuePolicy {
                max_total_items: 9,
                ..QueuePolicy::default()
            };
            let (mut supervisor, mut owner, mut turn, mut sink, bindings, _) =
                bound_boundary_fixture_with_policy(policy);
            let b = &bindings[1];
            let started = supervisor.start_commands(&mut turn);
            started.outcome.expect("start");
            dispatch_boundary(&mut owner, &mut turn, started.commands);
            if post_cut {
                let marker = external_failed_marker(&supervisor, 50, Reason::Unknown);
                sink.persist_marker(&mut turn, &marker, RecordingGate::Durable)
                    .expect("confirmed archive cut with no queued observations");
            }
            let bytes = vec![b'x'; policy.max_raw_message_bytes + 1];
            supervisor
                .queue_text(
                    &mut turn,
                    b.connection_id,
                    b.tag.connection,
                    stamp(60),
                    &bytes,
                )
                .outcome
                .expect("counted loss owner");
            let before = supervisor.retention_report().ownership.work_used;
            let coalesced = supervisor.admit_with_reserver(
                &mut turn,
                Some(b.connection_id),
                Some(b.tag.connection),
                stamp(61),
                ReceivedCall::Text(&bytes),
                |_, _| panic!("same-side F2 must not reserve even at AdmissionOrder::MAX"),
            );
            assert_eq!(coalesced.outcome, Ok(AdmissionOutcome::CoalescedLoss));
            assert!(coalesced.failure.is_none());
            assert_eq!(supervisor.retention_report().ownership.work_used, before);
            assert_eq!(
                supervisor
                    .snapshot(b.id)
                    .expect("neighbor")
                    .capture_attempt_frontier,
                2
            );
            assert!(
                matches!(supervisor.core.queue.back(), Some(Ingress::QueueGap {
                first_attempt, last_attempt, loss_count: 2, stamp: original, ..
            }) if first_attempt.get() == 1 && last_attempt.get() == 2 && original.monotonic_ns == 60)
            );
            if !post_cut {
                let marker = external_failed_marker(&supervisor, 62, Reason::Unknown);
                assert!(matches!(
                    sink.persist_marker(&mut turn, &marker, RecordingGate::Durable),
                    Err(session::PersistBoundaryError::Authority(
                        session::AuthorityError::NotQuiescent
                    ))
                ));
                let mut reserve_calls = 0;
                let terminal = supervisor.admit_with_reserver(
                    &mut turn,
                    Some(b.connection_id),
                    Some(b.tag.connection),
                    stamp(63),
                    ReceivedCall::Text(&bytes),
                    |_, _| {
                        reserve_calls += 1;
                        Err(session::AuthorityError::CounterExhausted("AdmissionOrder"))
                    },
                );
                assert_eq!(reserve_calls, 1, "PreCut GAP cannot serve post-cut loss");
                assert_eq!(
                    terminal.failure.expect("exact terminal").attempt,
                    session::AttemptIdentity::Candidate(
                        CaptureAttemptNo::new(3).expect("candidate")
                    )
                );
                assert!(
                    matches!(supervisor.core.queue.back(), Some(Ingress::QueueGap {
                    last_attempt, loss_count: 2, stamp: original, ..
                }) if last_attempt.get() == 2 && original.monotonic_ns == 60)
                );
                dispatch_boundary(&mut owner, &mut turn, terminal.commands);
            }
        }
    }
}
