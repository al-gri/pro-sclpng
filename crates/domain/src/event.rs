//! Source/application/control identities and structural, causal envelope guards.
//! Inputs and history are supplied by the caller; there is no decoder, clock,
//! queue, book engine, network connection or replay loop in this module.

use std::collections::BTreeSet;
use std::fmt;

use crate::artifact::{ArtifactError, ArtifactRef, validate_sorted_refs};
use crate::identity::*;
use crate::numeric::{NumericError, PriceTicks, QuantitySteps};

pub const MAX_BOOK_ENTRIES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventError {
    Identity(IdentityError),
    Numeric(NumericError),
    Artifact(ArtifactError),
    UnsupportedSchema,
    InvalidBootstrapContext,
    ContextMismatch,
    RecordOrderError,
    SubEventOrderError,
    FutureCausalReference,
    MissingCausalReference,
    CausalFrontierMismatch,
    MissingRawInput,
    SourceMismatch,
    AvailabilityMismatch,
    IncomparableClock,
    TimeOverflow,
    TimeOrderError,
    EventTooLarge,
    DuplicateLevel,
    SnapshotOrderError,
    MixedFrameUnsupported,
}

impl From<IdentityError> for EventError {
    fn from(error: IdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<NumericError> for EventError {
    fn from(error: NumericError) -> Self {
        Self::Numeric(error)
    }
}

impl From<ArtifactError> for EventError {
    fn from(error: ArtifactError) -> Self {
        Self::Artifact(error)
    }
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for EventError {}

type Result<T> = std::result::Result<T, EventError>;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecordRef {
    pub archive: ArchiveId,
    pub record: RecordNo,
}

pub type InputCursor = RecordRef;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawFrameId {
    pub archive: ArchiveId,
    pub record: RecordNo,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SourceCandidateKey {
    pub raw: RawFrameId,
    pub index: RawSubIndex,
    pub normalizer: NormalizerVersion,
}

pub type SourceApplicationKey = SourceCandidateKey;

/// Lexicographic order is meaningful only within its enclosing ArchiveId.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventCursor {
    pub apply_record: RecordNo,
    pub output_index: OutputIndex,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EventId {
    pub archive: ArchiveId,
    pub cursor: EventCursor,
    pub normalizer: NormalizerVersion,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EventRef {
    pub archive: ArchiveId,
    pub cursor: EventCursor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveContext {
    pub config: ConfigVersion,
    pub normalizer: NormalizerVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputContext {
    Bootstrap,
    Active(ActiveContext),
}

impl InputContext {
    /// Decode only Context-bearing kinds 2..7. Zero values never become
    /// ordinary version newtypes; Bootstrap is administrative and pre-config.
    pub fn decode(config: u32, normalizer: u32, kind: u16, has_active: bool) -> Result<Self> {
        if !(2..=7).contains(&kind) {
            return Err(EventError::InvalidBootstrapContext);
        }
        if config == 0 || normalizer == 0 {
            if config == 0 && normalizer == 0 && (2..=4).contains(&kind) && !has_active {
                return Ok(Self::Bootstrap);
            }
            return Err(EventError::InvalidBootstrapContext);
        }
        Ok(Self::Active(ActiveContext {
            config: ConfigVersion::new(config)?,
            normalizer: NormalizerVersion::new(normalizer)?,
        }))
    }
}

/// Read-only definition history, not a requirement to keep an unbounded map
/// inside a production reducer. Definitions and early-loaded artifacts differ.
pub trait DefinitionHistory {
    fn config_defined(&self, config: ConfigVersion) -> bool;
    fn normalizer_reference(&self, normalizer: NormalizerVersion) -> Option<ArtifactRef>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContextTimeline {
    active: Option<ActiveContext>,
    effective_from: Option<RecordNo>,
}

impl ContextTimeline {
    pub fn active(&self) -> Option<ActiveContext> {
        self.active
    }

    pub fn check(&self, record: RecordNo, context: InputContext) -> Result<()> {
        if self.effective_from.is_some_and(|first| record < first) {
            return Err(EventError::ContextMismatch);
        }
        let expected = self.active.map_or(InputContext::Bootstrap, InputContext::Active);
        if context != expected {
            return Err(EventError::ContextMismatch);
        }
        Ok(())
    }

    /// Call after policy and artifact applicability guards. Validation occurs
    /// before mutation; this checks timeline/identity, not artifact authenticity.
    pub fn activate(
        &mut self,
        record: RecordNo,
        context: InputContext,
        next: ActiveContext,
        normalizer_ref: ArtifactRef,
        history: &impl DefinitionHistory,
    ) -> Result<()> {
        self.check(record, context)?;
        if history.config_defined(next.config) {
            return Err(IdentityError::IdentityConflict.into());
        }
        if history
            .normalizer_reference(next.normalizer)
            .is_some_and(|old| old != normalizer_ref)
        {
            return Err(ArtifactError::ArtifactIdentityConflict.into());
        }
        let first = record.checked_next()?;
        self.active = Some(next);
        self.effective_from = Some(first);
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordedOrder {
    last: Option<RecordNo>,
}

impl RecordedOrder {
    pub fn last(&self) -> Option<RecordNo> {
        self.last
    }

    pub fn admit(&mut self, next: RecordNo) -> Result<()> {
        let expected = match self.last {
            Some(last) => last.checked_next()?.get(),
            None => 1,
        };
        if next.get() != expected {
            return Err(EventError::RecordOrderError);
        }
        self.last = Some(next);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockScope {
    pub session: CaptureSessionId,
    pub clock: ClockId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MonotonicSample {
    pub scope: ClockScope,
    pub ns: MonotonicNs,
}

impl MonotonicSample {
    pub fn elapsed_since(self, earlier: Self) -> Result<DurationNs> {
        if self.scope != earlier.scope {
            return Err(EventError::IncomparableClock);
        }
        self.ns
            .get()
            .checked_sub(earlier.ns.get())
            .map(DurationNs::new)
            .ok_or(EventError::TimeOrderError)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveSample {
    pub unix_ns: LocalUnixNs,
    pub monotonic: MonotonicSample,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TimestampUnit {
    Seconds = 1,
    Millis = 2,
    Micros = 3,
    Nanos = 4,
}

impl TimestampUnit {
    pub fn to_nanoseconds(self, value: i64) -> Result<i64> {
        let factor = match self {
            Self::Seconds => 1_000_000_000,
            Self::Millis => 1_000_000,
            Self::Micros => 1000,
            Self::Nanos => 1,
        };
        value.checked_mul(factor).ok_or(EventError::TimeOverflow)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceTimestamp {
    Unknown,
    Known { value: i64, unit: TimestampUnit, origin: Token<32> },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum Side {
    Bid = 1,
    Ask = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Level {
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: QuantitySteps,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LevelChange {
    Set(Level),
    Delete { side: Side, price: PriceTicks },
}

impl LevelChange {
    pub fn key(&self) -> (Side, PriceTicks) {
        match self {
            Self::Set(level) => (level.side, level.price),
            Self::Delete { side, price } => (*side, *price),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Aggressor {
    Unknown,
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpiAttribute {
    Unknown,
    Yes,
    No,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TradeBookLink {
    Unknown,
    Proven { book_event: EventRef, evidence: RecordRef, proof: ArtifactRef },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trade {
    pub price: PriceTicks,
    pub quantity: QuantitySteps,
    pub aggressor: Aggressor,
    pub rpi: RpiAttribute,
    pub source_trade_id: Option<Token<128>>,
    pub book_link: TradeBookLink,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketPayload {
    Snapshot(Vec<Level>),
    Update(Vec<LevelChange>),
    Trade(Trade),
}

impl MarketPayload {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Snapshot(levels) => {
                if levels.len() > MAX_BOOK_ENTRIES {
                    return Err(EventError::EventTooLarge);
                }
                let mut keys = BTreeSet::new();
                let mut last_bid = None;
                let mut last_ask = None;
                for level in levels {
                    level.quantity.for_level()?;
                    if !keys.insert((level.side, level.price)) {
                        return Err(EventError::DuplicateLevel);
                    }
                    match level.side {
                        Side::Bid => {
                            if last_ask.is_some()
                                || last_bid.is_some_and(|last| level.price >= last)
                            {
                                return Err(EventError::SnapshotOrderError);
                            }
                            last_bid = Some(level.price);
                        }
                        Side::Ask => {
                            if last_ask.is_some_and(|last| level.price <= last) {
                                return Err(EventError::SnapshotOrderError);
                            }
                            last_ask = Some(level.price);
                        }
                    }
                }
            }
            Self::Update(changes) => {
                if changes.len() > MAX_BOOK_ENTRIES {
                    return Err(EventError::EventTooLarge);
                }
                let mut keys = BTreeSet::new();
                for change in changes {
                    if let LevelChange::Set(level) = change {
                        level.quantity.for_level()?;
                    }
                    if !keys.insert(change.key()) {
                        return Err(EventError::DuplicateLevel);
                    }
                }
            }
            Self::Trade(trade) => {
                trade.quantity.for_trade()?;
            }
        }
        Ok(())
    }

    pub fn two_sided_snapshot(&self) -> bool {
        let Self::Snapshot(levels) = self else {
            return false;
        };
        levels.iter().any(|level| level.side == Side::Bid)
            && levels.iter().any(|level| level.side == Side::Ask)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedOutput {
    pub payload: MarketPayload,
    pub timestamp: SourceTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceMetadata {
    pub raw: RawFrameId,
    pub binding: StreamBinding,
    pub context: ActiveContext,
    pub received: ReceiveSample,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedFrame {
    pub source: SourceMetadata,
    pub raw_byte_len: u32,
    pub outputs: Vec<NormalizedOutput>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameKind {
    NoMarketData,
    Snapshot,
    Delta,
    Trades,
}

impl NormalizedFrame {
    pub fn validate(&self) -> Result<FrameKind> {
        self.source.binding.validate()?;
        if self.outputs.is_empty() {
            return Ok(FrameKind::NoMarketData);
        }
        u32::try_from(self.outputs.len()).map_err(|_| EventError::EventTooLarge)?;
        let trades = self.source.binding.channel == Channel::Trades;
        let kind = match &self.outputs[0].payload {
            MarketPayload::Snapshot(_) if !trades && self.outputs.len() == 1 => FrameKind::Snapshot,
            MarketPayload::Update(_) if !trades => FrameKind::Delta,
            MarketPayload::Trade(_) if trades => FrameKind::Trades,
            _ => return Err(EventError::MixedFrameUnsupported),
        };
        for output in &self.outputs {
            let compatible = matches!(
                (&output.payload, kind),
                (MarketPayload::Snapshot(_), FrameKind::Snapshot)
                    | (MarketPayload::Update(_), FrameKind::Delta)
                    | (MarketPayload::Trade(_), FrameKind::Trades)
            );
            if !compatible {
                return Err(EventError::MixedFrameUnsupported);
            }
            output.payload.validate()?;
        }
        Ok(kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CausalBasis {
    pub record_frontier: RecordNo,
    pub prior_effects: Vec<EventCursor>,
}

/// Caller-owned immutable input/effect view. Implementations must not fabricate
/// RawInput references for administrative records. Availability is checked below.
pub trait PrefixView {
    fn raw(&self, id: RawFrameId) -> Option<&NormalizedFrame>;
    fn record_exists(&self, record: RecordRef) -> bool;
    fn effect_exists(&self, event: EventRef) -> bool;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub event_id: EventId,
    pub source_candidate: SourceCandidateKey,
    pub raw_input_ref: RawFrameId,
    pub source_ingest_order: RecordNo,
    pub binding: StreamBinding,
    pub source_timestamp: SourceTimestamp,
    pub received: ReceiveSample,
    pub applied_at: MonotonicSample,
    pub context: ActiveContext,
    pub available_at: EventCursor,
    pub as_of: CausalBasis,
    pub record_schema_version: u16,
    pub artifact_refs: Vec<ArtifactRef>,
    pub payload: MarketPayload,
}

pub struct EnvelopeContext<'a> {
    pub apply: RecordRef,
    pub binding: &'a StreamBinding,
    pub active: ActiveContext,
    pub clock: ClockScope,
    pub artifact_refs: &'a [ArtifactRef],
    pub prior_outputs_this_step: &'a [EventCursor],
}

impl EventEnvelope {
    pub fn validate(&self, expected: &EnvelopeContext<'_>, view: &impl PrefixView) -> Result<()> {
        if self.schema_version != 1 || self.record_schema_version != 1 {
            return Err(EventError::UnsupportedSchema);
        }
        if self.event_id.archive != expected.apply.archive
            || self.raw_input_ref.archive != expected.apply.archive
            || self.event_id.cursor.apply_record != expected.apply.record
        {
            return Err(IdentityError::IdentityMismatch.into());
        }
        self.binding.validate()?;
        self.binding.spec.ensure_same(&expected.binding.spec)?;
        if self.binding.tag != expected.binding.tag {
            return Err(IdentityError::EpochMismatch.into());
        }
        if self.binding != *expected.binding {
            return Err(IdentityError::IdentityMismatch.into());
        }
        if self.context != expected.active || self.event_id.normalizer != self.context.normalizer {
            return Err(EventError::ContextMismatch);
        }
        if self.available_at != self.event_id.cursor {
            return Err(EventError::AvailabilityMismatch);
        }
        if self.as_of.record_frontier > expected.apply.record {
            return Err(EventError::FutureCausalReference);
        }
        if self.as_of.record_frontier != expected.apply.record {
            return Err(EventError::CausalFrontierMismatch);
        }
        if self.raw_input_ref.record > expected.apply.record {
            return Err(EventError::FutureCausalReference);
        }
        if !view.record_exists(expected.apply) {
            return Err(EventError::MissingCausalReference);
        }
        let raw = view.raw(self.raw_input_ref).ok_or(EventError::MissingRawInput)?;
        raw.validate()?;
        let key = self.source_candidate;
        if key.raw != self.raw_input_ref
            || self.source_ingest_order != self.raw_input_ref.record
            || key.normalizer != self.context.normalizer
            || raw.source.raw != self.raw_input_ref
            || raw.source.binding != self.binding
            || raw.source.context != self.context
            || raw.source.received != self.received
        {
            return Err(EventError::SourceMismatch);
        }
        let index = usize::try_from(key.index.get()).map_err(|_| EventError::SubEventOrderError)?;
        let source = raw.outputs.get(index).ok_or(EventError::SubEventOrderError)?;
        if source.payload != self.payload || source.timestamp != self.source_timestamp {
            return Err(EventError::SourceMismatch);
        }
        if self.received.monotonic.scope != expected.clock || self.applied_at.scope != expected.clock {
            return Err(EventError::IncomparableClock);
        }
        self.applied_at.elapsed_since(self.received.monotonic)?;
        validate_sorted_refs(&self.artifact_refs)?;
        if self.artifact_refs != expected.artifact_refs {
            return Err(ArtifactError::MissingArtifact.into());
        }
        let mut previous = None;
        for cursor in &self.as_of.prior_effects {
            if *cursor >= self.available_at {
                return Err(EventError::FutureCausalReference);
            }
            if previous.is_some_and(|last| *cursor <= last) {
                return Err(EventError::SubEventOrderError);
            }
            let exists = view.effect_exists(EventRef {
                archive: self.event_id.archive,
                cursor: *cursor,
            }) || expected.prior_outputs_this_step.contains(cursor);
            if !exists {
                return Err(EventError::MissingCausalReference);
            }
            previous = Some(*cursor);
        }
        if let MarketPayload::Trade(Trade {
            book_link: TradeBookLink::Proven { book_event, evidence, proof },
            ..
        }) = &self.payload
        {
            if book_event.archive != self.event_id.archive || evidence.archive != self.event_id.archive {
                return Err(IdentityError::IdentityMismatch.into());
            }
            if book_event.cursor >= self.available_at || evidence.record > expected.apply.record {
                return Err(EventError::FutureCausalReference);
            }
            if !view.effect_exists(*book_event)
                || !view.record_exists(*evidence)
                || !self.artifact_refs.contains(proof)
            {
                return Err(EventError::MissingCausalReference);
            }
        }
        self.payload.validate()
    }
}

pub fn validate_dense_outputs(apply: RecordRef, outputs: &[EventEnvelope]) -> Result<()> {
    for (index, event) in outputs.iter().enumerate() {
        let index = u32::try_from(index).map_err(|_| EventError::EventTooLarge)?;
        if event.event_id.archive != apply.archive
            || event.event_id.cursor.apply_record != apply.record
            || event.event_id.cursor.output_index.get() != index
        {
            return Err(EventError::SubEventOrderError);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    New,
    IdenticalDuplicate,
}

pub fn classify_delivery(old: Option<&EventEnvelope>, new: &EventEnvelope) -> Result<Delivery> {
    match old {
        None => Ok(Delivery::New),
        Some(old) if old.event_id != new.event_id => Err(IdentityError::IdentityMismatch.into()),
        Some(old) if old != new => Err(IdentityError::IdentityConflict.into()),
        Some(_) => Ok(Delivery::IdenticalDuplicate),
    }
}
