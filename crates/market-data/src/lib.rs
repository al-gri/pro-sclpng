//! Offline, bounded decoding for the accepted Bitget regular JSON fixture profile.
//!
//! This crate intentionally exposes wire-layer data and continuity diagnostics only.
//! It performs no I/O and does not mutate a local order book.

mod continuity;
mod decoder;
mod json;

pub use continuity::{ContinuityClassifier, ContinuityOutcome, ContinuityRule};
pub use decoder::{
    Action, BOOKS50_MAX_LEVELS, BitgetMessage, Books50Frame, Category, DecodeError, DecodeLimits,
    FillSide, LexicalValue, PublicTradeFrame, RpiFlag, Topic, WireLevel, WireTimestampMs, WireTrade,
    decode_message, decode_message_with_limits,
};
pub use json::{JsonError, JsonErrorKind};
