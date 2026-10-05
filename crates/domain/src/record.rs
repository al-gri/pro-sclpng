//! Value-only recorded-input DTOs. Byte codecs and filesystem operations are
//! intentionally absent. The memory codec used by contract tests is test-local.

use std::fmt;

use crate::artifact::ArtifactRef;
use crate::event::{ActiveContext, EventError, InputContext};
use crate::identity::*;
use crate::policy::{DurabilityMode, PolicyError, PolicyFields, WatermarkKind};
use crate::qualified::NumericSpec;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordError {
    Identity(IdentityError),
    Event(EventError),
    Policy(PolicyError),
    Unsupported { field: &'static str, value: u64 },
    InvalidPayload(&'static str),
    InvalidLossCount,
    InvalidLossScope,
    LossCountMismatch,
    LengthError,
}

impl From<IdentityError> for RecordError {
    fn from(value: IdentityError) -> Self {
        Self::Identity(value)
    }
}

impl From<EventError> for RecordError {
    fn from(value: EventError) -> Self {
        Self::Event(value)
    }
}

impl From<PolicyError> for RecordError {
    fn from(value: PolicyError) -> Self {
        Self::Policy(value)
    }
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RecordError {}

type Result<T> = std::result::Result<T, RecordError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum RecordKind {
    ArchiveStart = 1,
    InstrumentSpec = 2,
    StreamDefinition = 3,
    ConfigDefinition = 4,
    RawInput = 5,
    Control = 6,
    Gap = 7,
    SegmentSeal = 8,
    SegmentStart = 9,
    ArchiveSeal = 10,
}

impl RecordKind {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for RecordKind {
    type Error = RecordError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::ArchiveStart),
            2 => Ok(Self::InstrumentSpec),
            3 => Ok(Self::StreamDefinition),
            4 => Ok(Self::ConfigDefinition),
            5 => Ok(Self::RawInput),
            6 => Ok(Self::Control),
            7 => Ok(Self::Gap),
            8 => Ok(Self::SegmentSeal),
            9 => Ok(Self::SegmentStart),
            10 => Ok(Self::ArchiveSeal),
            _ => Err(RecordError::Unsupported {
                field: "record_kind",
                value: value.into(),
            }),
        }
    }
}

macro_rules! tags {
    ($name:ident, $field:literal, $($variant:ident = $tag:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(u8)]
        pub enum $name {
            $($variant = $tag),+
        }

        impl $name {
            pub const fn tag(self) -> u8 {
                self as u8
            }
        }

        impl TryFrom<u8> for $name {
            type Error = RecordError;

            fn try_from(value: u8) -> Result<Self> {
                match value {
                    $($tag => Ok(Self::$variant),)+
                    _ => Err(RecordError::Unsupported { field: $field, value: value.into() }),
                }
            }
        }
    };
}

tags!(
    ProvenanceKind,
    "provenance_kind",
    Engineering = 1,
    Synthetic = 2,
    SourceVerified = 3
);
tags!(Transport, "Transport.liveness", Unknown = 0, Up = 1, Down = 2);
tags!(
    BookEvidenceKind,
    "Verification.evidence_kind",
    Snapshot = 1,
    Delta = 2
);
tags!(
    Freshness,
    "FreshnessEvidence.freshness",
    Unknown = 0,
    Fresh = 1,
    QuietVerified = 2,
    Stale = 3
);
tags!(
    RecordingHealth,
    "RecordingEvidence.health",
    Unknown = 0,
    Healthy = 1,
    Degraded = 2,
    Failed = 3
);
tags!(
    Reason,
    "reason",
    UserReset = 1,
    Reconnect = 2,
    SourceGap = 3,
    QueueOverflow = 4,
    DecodeRejected = 5,
    WriteFailure = 6,
    NoFault = 7,
    Unknown = 255
);
tags!(
    InputQuality,
    "ArchiveSeal.input_quality",
    NoKnownLoss = 1,
    GapsRecorded = 2,
    Unknown = 3
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireContext {
    pub unix_ns: LocalUnixNs,
    pub monotonic_ns: MonotonicNs,
    pub context: InputContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveStart {
    pub archive: ArchiveId,
    pub session: CaptureSessionId,
    pub clock: ClockId,
    pub mode: DurabilityMode,
    pub previous_archive: Option<ArchiveId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSpecRecord {
    pub context: WireContext,
    pub slot: InstrumentSlot,
    pub numeric: NumericSpec,
    pub provenance: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamDefinition {
    pub context: WireContext,
    pub binding: StreamBinding,
    pub provenance: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigDefinition {
    pub context: WireContext,
    pub next: ActiveContext,
    pub provenance_kind: ProvenanceKind,
    pub evidence: ArtifactRef,
    pub fields: PolicyFields,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawInput {
    pub context: WireContext,
    pub stream: StreamId,
    pub tag: EpochTag,
    pub attempt: CaptureAttemptNo,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpochChange {
    Connection {
        owner: ConnectionId,
        expected: ConnectionEpoch,
        next: ConnectionEpoch,
    },
    Subscription {
        owner: StreamId,
        expected: SubscriptionEpoch,
        next: SubscriptionEpoch,
    },
    Book {
        owner: BookId,
        expected: BookEpoch,
        next: BookEpoch,
    },
}

impl EpochChange {
    pub fn wire_parts(&self) -> (u8, u32, u64, u64) {
        match *self {
            Self::Connection {
                owner,
                expected,
                next,
            } => (1, owner.get(), expected.get(), next.get()),
            Self::Subscription {
                owner,
                expected,
                next,
            } => (2, owner.get(), expected.get(), next.get()),
            Self::Book {
                owner,
                expected,
                next,
            } => (3, owner.get(), expected.get(), next.get()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationEvidence {
    pub stream: StreamId,
    pub tag: EpochTag,
    pub raw: RecordNo,
    pub kind: BookEvidenceKind,
    pub profile: FeedProfileVersion,
    pub proof: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupEvidence {
    pub stream: StreamId,
    pub tag: EpochTag,
    pub anchor: RecordNo,
    pub update_count: u32,
    pub elapsed_ns: u64,
    pub proof: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshnessEvidence {
    pub stream: StreamId,
    pub tag: EpochTag,
    pub freshness: Freshness,
    pub basis: Option<RecordNo>,
    pub proof: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordingEvidence {
    pub health: RecordingHealth,
    pub kind: WatermarkKind,
    pub through: Option<RecordNo>,
    pub reason: Reason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Control {
    Timer {
        stream: StreamId,
        timer_id: u64,
        deadline_ns: u64,
    },
    Transport {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        value: Transport,
    },
    EpochAdvance {
        change: EpochChange,
        reason: Reason,
    },
    SpecActivate {
        slot: InstrumentSlot,
        expected: SpecVersion,
        next: SpecVersion,
    },
    Verification(VerificationEvidence),
    Warmup(WarmupEvidence),
    Freshness(FreshnessEvidence),
    Recording(RecordingEvidence),
}

impl Control {
    pub const fn tag(&self) -> u8 {
        match self {
            Self::Timer { .. } => 1,
            Self::Transport { .. } => 2,
            Self::EpochAdvance { .. } => 3,
            Self::SpecActivate { .. } => 4,
            Self::Verification(_) => 5,
            Self::Warmup(_) => 6,
            Self::Freshness(_) => 7,
            Self::Recording(_) => 8,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRecord {
    pub context: WireContext,
    pub value: Control,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GapTarget {
    pub stream: StreamId,
    pub tag: EpochTag,
    pub range: Option<(CaptureAttemptNo, CaptureAttemptNo)>,
    pub loss_count: Option<u64>,
}

impl GapTarget {
    pub fn validate(&self, reason: Reason) -> Result<()> {
        if self.loss_count == Some(0) {
            return Err(RecordError::InvalidLossCount);
        }
        if reason != Reason::QueueOverflow && (self.range.is_some() || self.loss_count.is_some()) {
            return Err(RecordError::InvalidLossScope);
        }
        if let Some((first, last)) = self.range {
            let count = last
                .get()
                .checked_sub(first.get())
                .and_then(|n| n.checked_add(1));
            let count = count.ok_or(RecordError::InvalidLossCount)?;
            if self.loss_count != Some(count) {
                return Err(RecordError::LossCountMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GapScope {
    ExplicitTargets(Vec<GapTarget>),
    AllDeclaredStreams,
}

impl GapScope {
    pub const fn tag(&self) -> u8 {
        match self {
            Self::ExplicitTargets(_) => 1,
            Self::AllDeclaredStreams => 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Gap {
    pub context: WireContext,
    pub scope: GapScope,
    pub reason: Reason,
}

impl Gap {
    pub fn validate(&self) -> Result<()> {
        if self.reason == Reason::NoFault {
            return Err(RecordError::InvalidPayload("Gap.reason"));
        }
        if let GapScope::ExplicitTargets(targets) = &self.scope {
            if targets.is_empty() || targets.len() > 256 {
                return Err(RecordError::InvalidPayload("Gap.target_count"));
            }
            if targets
                .windows(2)
                .any(|pair| pair[0].stream >= pair[1].stream)
            {
                return Err(RecordError::InvalidPayload("Gap.targets_order"));
            }
            for target in targets {
                target.validate(self.reason)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentSeal {
    pub prefix_frame_count: u64,
    pub prefix_physical_len: u64,
    pub prefix_crc32: u32,
    pub prior_record: RecordNo,
    pub has_gap: bool,
    pub is_final: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentStart {
    pub archive: ArchiveId,
    pub session: CaptureSessionId,
    pub clock: ClockId,
    pub previous_segment: SegmentNo,
    pub previous_seal_record: RecordNo,
    pub previous_seal_crc32: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveSeal {
    pub expected_segment_count: u32,
    pub prior_frame_count: u64,
    pub total_prefix_physical_bytes: u64,
    pub prefix_crc32: u32,
    pub prior_record: RecordNo,
    pub input_quality: InputQuality,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Record {
    ArchiveStart(ArchiveStart),
    InstrumentSpec(InstrumentSpecRecord),
    StreamDefinition(StreamDefinition),
    ConfigDefinition(ConfigDefinition),
    RawInput(RawInput),
    Control(ControlRecord),
    Gap(Gap),
    SegmentSeal(SegmentSeal),
    SegmentStart(SegmentStart),
    ArchiveSeal(ArchiveSeal),
}

impl Record {
    pub const fn kind(&self) -> RecordKind {
        match self {
            Self::ArchiveStart(_) => RecordKind::ArchiveStart,
            Self::InstrumentSpec(_) => RecordKind::InstrumentSpec,
            Self::StreamDefinition(_) => RecordKind::StreamDefinition,
            Self::ConfigDefinition(_) => RecordKind::ConfigDefinition,
            Self::RawInput(_) => RecordKind::RawInput,
            Self::Control(_) => RecordKind::Control,
            Self::Gap(_) => RecordKind::Gap,
            Self::SegmentSeal(_) => RecordKind::SegmentSeal,
            Self::SegmentStart(_) => RecordKind::SegmentStart,
            Self::ArchiveSeal(_) => RecordKind::ArchiveSeal,
        }
    }

    pub const fn context(&self) -> Option<&WireContext> {
        match self {
            Self::InstrumentSpec(value) => Some(&value.context),
            Self::StreamDefinition(value) => Some(&value.context),
            Self::ConfigDefinition(value) => Some(&value.context),
            Self::RawInput(value) => Some(&value.context),
            Self::Control(value) => Some(&value.context),
            Self::Gap(value) => Some(&value.context),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordFrame {
    pub record_no: RecordNo,
    pub segment_no: SegmentNo,
    pub value: Record,
}

impl RecordFrame {
    pub fn validate_shape(&self) -> Result<()> {
        let bootstrap = self
            .value
            .context()
            .is_some_and(|context| context.context == InputContext::Bootstrap);
        if bootstrap && !(2..=4).contains(&self.value.kind().tag()) {
            return Err(EventError::InvalidBootstrapContext.into());
        }
        match &self.value {
            Record::ArchiveStart(start) => {
                if self.record_no.get() != 1 || self.segment_no.get() != 0 {
                    return Err(RecordError::InvalidPayload("ArchiveStart.position"));
                }
                if start.previous_archive == Some(start.archive) {
                    return Err(RecordError::InvalidPayload("ArchiveStart.previous_archive"));
                }
            }
            Record::StreamDefinition(value) => value.binding.validate()?,
            Record::ConfigDefinition(value) => value.fields.validate(DurabilityMode::Buffered)?,
            Record::RawInput(value) => {
                if value.bytes.len() > 1_048_576 {
                    return Err(RecordError::LengthError);
                }
            }
            Record::Control(value) => match &value.value {
                Control::Timer {
                    timer_id,
                    deadline_ns,
                    ..
                } => {
                    if *timer_id == 0 || value.context.monotonic_ns.get() < *deadline_ns {
                        return Err(RecordError::InvalidPayload("TimerFired.deadline"));
                    }
                }
                Control::EpochAdvance { change, reason } => {
                    let (_, _, expected, next) = change.wire_parts();
                    if next <= expected {
                        return Err(IdentityError::EpochRollback.into());
                    }
                    if *reason == Reason::NoFault {
                        return Err(RecordError::InvalidPayload("EpochAdvance.reason"));
                    }
                }
                Control::SpecActivate { expected, next, .. } => {
                    if next <= expected {
                        return Err(IdentityError::SpecMismatch.into());
                    }
                }
                Control::Recording(evidence)
                    if evidence.reason == Reason::NoFault
                        && evidence.health != RecordingHealth::Healthy =>
                {
                    return Err(RecordError::InvalidPayload("RecordingEvidence.reason"));
                }
                _ => {}
            },
            Record::Gap(value) => value.validate()?,
            _ => {}
        }
        Ok(())
    }
}
