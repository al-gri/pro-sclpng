//! Bounded market-data decoding, deterministic DataHealth reduction, and public WS supervision.
//!
//! REC-001D owns the Bitget public WebSocket protocol/supervision state machine and
//! accepted WAL-record construction. Concrete TLS/socket I/O, live clock sampling,
//! canonical local-book quantities/levels, private APIs, and trading remain outside
//! this crate.

mod continuity;
mod data_health;
mod decoder;
mod json;
mod ws_supervisor;

pub use continuity::{ContinuityClassifier, ContinuityOutcome, ContinuityRule};
pub use data_health::{
    BookFrameObservation, BookInvalidReason, BookValidity, ConnectionTransportSnapshot,
    ContinuityReport, DataHealthReducer, DataHealthSnapshot, HealthDiagnostic, HealthEffect,
    HealthError, HealthObservation, PendingLimit, PendingSummary, RecordedHealthObservation,
    StepResult, StreamHealthSnapshot, VerifiedFrameProof, VerifiedProofConflict,
    VerifiedWarmupProof,
};
pub use decoder::{
    Action, BOOKS50_MAX_LEVELS, BitgetMessage, Books50Frame, Category, DecodeError, DecodeLimits,
    FillSide, LexicalValue, PublicTradeFrame, RpiFlag, Topic, WireLevel, WireTimestampMs,
    WireTrade, decode_message, decode_message_with_limits,
};
pub use json::{JsonError, JsonErrorKind};
pub use ws_supervisor::{
    BITGET_PUBLIC_WS_ENDPOINT, DrainResult, HEARTBEAT_INTERVAL_NS, MAX_CONFIGURED_STREAMS,
    PONG_TIMEOUT_NS_V1, PersistError, PersistenceReceipt, PublicWsSupervisor, QueuePolicy,
    RECONNECT_BASE_NS_V1, RECONNECT_MAX_NS_V1, ReceiveStamp, RecordSink, SUPERVISOR_POLICY_VERSION,
    StreamSupervisorSnapshot, SubscriptionState, SupervisorError, SupervisorEvent,
    TransportCommand, WsSupervisorConfig, reconnect_delay_ns,
};
