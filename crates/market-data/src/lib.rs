//! Offline, bounded market-data decoding and deterministic DataHealth reduction.
//!
//! This crate intentionally owns no network connection, live clock, storage I/O
//! or canonical local-book quantities/levels.

mod continuity;
mod data_health;
mod decoder;
mod json;

pub use continuity::{ContinuityClassifier, ContinuityOutcome, ContinuityRule};
pub use data_health::{
    BookFrameObservation, BookInvalidReason, BookValidity, ConnectionTransportSnapshot,
    ContinuityReport, DataHealthReducer, DataHealthSnapshot, HealthDiagnostic, HealthEffect,
    HealthError, HealthObservation, PendingLimit, PendingSummary, RecordedHealthObservation,
    StepResult, StreamHealthSnapshot,
};
pub use decoder::{
    Action, BOOKS50_MAX_LEVELS, BitgetMessage, Books50Frame, Category, DecodeError, DecodeLimits,
    FillSide, LexicalValue, PublicTradeFrame, RpiFlag, Topic, WireLevel, WireTimestampMs,
    WireTrade, decode_message, decode_message_with_limits,
};
pub use json::{JsonError, JsonErrorKind};
