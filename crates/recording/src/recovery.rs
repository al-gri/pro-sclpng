use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::policy::{DurabilityMode, PolicyError, WatermarkKind};
use domain::record::*;

use crate::binary::{CodecError, CodecErrorKind, Crc32};
use crate::codec::{decode_frame, parse_header, scan_frame, Definitions, HEADER_LEN};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveStatus {
    NoArchive,
    ValidPrefixIncomplete,
    SegmentSealedArchiveIncomplete,
    TruncatedTail,
    Corrupt,
    Unsupported,
    Invalid,
    Complete,
}

/// Artifact resolution and market reducer applicability are intentionally a
/// downstream concern. A physical Complete result never means canonical-ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalStatus {
    NotEvaluated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LossError {
    Shape(RecordError),
    LossOverlap,
    LossCoverageGap,
    LossCountMismatch,
    AmbiguousLossWindow,
    UnaccountedAttemptGap,
    AttemptOrderError,
    GapScopeTransition,
    UnresolvedLossWindow,
    AttemptCounterExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    Record(RecordError),
    Identity(IdentityError),
    Policy(PolicyError),
    ContextMismatch,
    UnknownDefinition(&'static str),
    OrderOrChain(&'static str),
    InvalidAcknowledgement,
    WatermarkRegression,
    WatermarkOrderError,
    Loss(LossError),
    TrailingData,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ValidationError {}

impl From<RecordError> for ValidationError {
    fn from(value: RecordError) -> Self {
        Self::Record(value)
    }
}

impl From<IdentityError> for ValidationError {
    fn from(value: IdentityError) -> Self {
        Self::Identity(value)
    }
}

impl From<PolicyError> for ValidationError {
    fn from(value: PolicyError) -> Self {
        Self::Policy(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailureKind {
    Codec(CodecError),
    Validation(ValidationError),
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    pub segment_index: usize,
    pub local_frame_start: u64,
    pub absolute_frame_start: u64,
    pub kind: FailureKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalReport {
    pub status: ArchiveStatus,
    pub canonical_status: CanonicalStatus,
    pub input_quality: Option<InputQuality>,
    pub physical_good_offset: u64,
    pub framing_good_offset: u64,
    pub last_record: Option<RecordNo>,
    pub failure: Option<Failure>,
}

impl Default for PhysicalReport {
    fn default() -> Self {
        Self {
            status: ArchiveStatus::NoArchive,
            canonical_status: CanonicalStatus::NotEvaluated,
            input_quality: None,
            physical_good_offset: 0,
            framing_good_offset: 0,
            last_record: None,
            failure: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadArchive {
    pub records: Vec<RecordFrame>,
    pub report: PhysicalReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LossWindow {
    left: u64,
    tag: EpochTag,
    recorded_count: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LossState {
    accounted_frontier: u64,
    window: Option<LossWindow>,
}

impl LossState {
    fn gap(
        &mut self,
        target: &GapTarget,
        reason: Reason,
        current: EpochTag,
    ) -> Result<(), LossError> {
        target.validate(reason).map_err(LossError::Shape)?;
        if reason != Reason::QueueOverflow {
            return Ok(());
        }
        if target.tag != current {
            return Err(LossError::GapScopeTransition);
        }
        if self.window.is_some() {
            return Err(LossError::AmbiguousLossWindow);
        }
        let expected = self
            .accounted_frontier
            .checked_add(1)
            .ok_or(LossError::AttemptCounterExhausted)?;
        if let Some((first, last)) = target.range {
            if first.get() <= self.accounted_frontier {
                return Err(LossError::LossOverlap);
            }
            if first.get() != expected {
                return Err(LossError::LossCoverageGap);
            }
            self.accounted_frontier = last.get();
        } else {
            self.window = Some(LossWindow {
                left: self.accounted_frontier,
                tag: target.tag,
                recorded_count: target.loss_count,
            });
        }
        Ok(())
    }

    fn raw(&mut self, attempt: CaptureAttemptNo, tag: EpochTag) -> Result<(), LossError> {
        let expected = self
            .accounted_frontier
            .checked_add(1)
            .ok_or(LossError::AttemptCounterExhausted)?;
        let value = attempt.get();
        if value <= self.accounted_frontier {
            return Err(LossError::AttemptOrderError);
        }
        if let Some(window) = &self.window {
            if window.tag != tag {
                return Err(LossError::GapScopeTransition);
            }
            let missing = value
                .checked_sub(window.left)
                .and_then(|distance| distance.checked_sub(1))
                .ok_or(LossError::AttemptOrderError)?;
            if window.recorded_count.is_some_and(|count| count != missing) {
                return Err(LossError::LossCountMismatch);
            }
        } else if value != expected {
            return Err(LossError::UnaccountedAttemptGap);
        }
        self.accounted_frontier = value;
        self.window = None;
        Ok(())
    }

    fn scope_change(&self) -> Result<(), LossError> {
        if self.window.is_some() {
            return Err(LossError::GapScopeTransition);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct StreamState {
    binding: StreamBinding,
    loss: LossState,
}

#[derive(Clone, Copy, Debug)]
struct SealLink {
    segment: SegmentNo,
    record: RecordNo,
    checksum: u32,
    final_segment: bool,
}

#[derive(Clone, Debug, Default)]
struct ObservedWatermarks {
    accepted: Option<RecordNo>,
    appended: Option<RecordNo>,
    written: Option<RecordNo>,
    flushed: Option<RecordNo>,
    durable: Option<RecordNo>,
}

impl ObservedWatermarks {
    fn values(&self) -> [Option<RecordNo>; 5] {
        [
            self.accepted,
            self.appended,
            self.written,
            self.flushed,
            self.durable,
        ]
    }

    fn observe(&mut self, kind: WatermarkKind, through: RecordNo) -> Result<(), ValidationError> {
        let index = match kind {
            WatermarkKind::Accepted => 0,
            WatermarkKind::Appended => 1,
            WatermarkKind::Written => 2,
            WatermarkKind::Flushed => 3,
            WatermarkKind::Durable => 4,
        };
        let values = self.values();
        if values[index].is_some_and(|old| through < old) {
            return Err(ValidationError::WatermarkRegression);
        }
        if values[index + 1..]
            .iter()
            .flatten()
            .any(|stronger| *stronger > through)
        {
            return Err(ValidationError::WatermarkOrderError);
        }

        let mut promoted = values;
        for slot in &mut promoted[..=index] {
            if slot.is_none_or(|old| through > old) {
                *slot = Some(through);
            }
        }
        self.accepted = promoted[0];
        self.appended = promoted[1];
        self.written = promoted[2];
        self.flushed = promoted[3];
        self.durable = promoted[4];
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArchiveValidator {
    start: Option<ArchiveStart>,
    mode: Option<DurabilityMode>,
    active: Option<ActiveContext>,
    configs: BTreeSet<ConfigVersion>,
    specs: Definitions,
    slot_identity: BTreeMap<InstrumentSlot, InstrumentRef>,
    identity_slot: BTreeMap<InstrumentRef, InstrumentSlot>,
    active_spec: BTreeMap<InstrumentSlot, SpecVersion>,
    streams: BTreeMap<StreamId, StreamState>,
    bindings: Vec<StreamBinding>,
    watermarks: ObservedWatermarks,
    current_segment: usize,
    local_count: u64,
    local_bytes: u64,
    local_crc: Crc32,
    local_gap: bool,
    global_count: u64,
    global_crc: Crc32,
    any_gap: bool,
    last_record: Option<RecordNo>,
    last_kind: Option<RecordKind>,
    last_seal: Option<SealLink>,
    archive_sealed: bool,
    input_quality: Option<InputQuality>,
}

impl Default for ArchiveValidator {
    fn default() -> Self {
        Self {
            start: None,
            mode: None,
            active: None,
            configs: BTreeSet::new(),
            specs: BTreeMap::new(),
            slot_identity: BTreeMap::new(),
            identity_slot: BTreeMap::new(),
            active_spec: BTreeMap::new(),
            streams: BTreeMap::new(),
            bindings: Vec::new(),
            watermarks: ObservedWatermarks::default(),
            current_segment: 0,
            local_count: 0,
            local_bytes: 0,
            local_crc: Crc32::default(),
            local_gap: false,
            global_count: 0,
            global_crc: Crc32::default(),
            any_gap: false,
            last_record: None,
            last_kind: None,
            last_seal: None,
            archive_sealed: false,
            input_quality: None,
        }
    }
}

impl ArchiveValidator {
    pub(crate) fn has_active(&self) -> bool {
        self.active.is_some()
    }

    pub(crate) fn definitions(&self) -> &Definitions {
        &self.specs
    }

    pub(crate) fn last_record(&self) -> Option<RecordNo> {
        self.last_record
    }

    pub(crate) fn archive_sealed(&self) -> bool {
        self.archive_sealed
    }

    pub(crate) fn can_rotate(&self) -> bool {
        self.last_seal
            .is_some_and(|seal| seal.segment.get() as usize == self.current_segment && !seal.final_segment)
    }

    pub(crate) fn eof_status(&self) -> ArchiveStatus {
        if self.global_count == 0 {
            ArchiveStatus::NoArchive
        } else if self.archive_sealed {
            ArchiveStatus::Complete
        } else if self.last_kind == Some(RecordKind::SegmentSeal) {
            ArchiveStatus::SegmentSealedArchiveIncomplete
        } else {
            ArchiveStatus::ValidPrefixIncomplete
        }
    }

    pub(crate) fn input_quality(&self) -> Option<InputQuality> {
        self.input_quality
    }

    fn check_context(&self, context: &WireContext) -> Result<(), ValidationError> {
        let expected = self
            .active
            .map_or(InputContext::Bootstrap, InputContext::Active);
        if context.context != expected {
            return Err(ValidationError::ContextMismatch);
        }
        Ok(())
    }

    fn check_record_reference(
        record: RecordNo,
        referenced: RecordNo,
    ) -> Result<(), ValidationError> {
        if referenced >= record {
            return Err(ValidationError::OrderOrChain("future_record_reference"));
        }
        Ok(())
    }

    fn check_open_windows<'a>(
        streams: impl Iterator<Item = &'a StreamState>,
    ) -> Result<(), ValidationError> {
        for stream in streams {
            stream
                .loss
                .scope_change()
                .map_err(ValidationError::Loss)?;
        }
        Ok(())
    }

    fn move_to_segment(&mut self, segment_index: usize) -> Result<(), ValidationError> {
        if segment_index == self.current_segment {
            return Ok(());
        }
        let next = self
            .current_segment
            .checked_add(1)
            .ok_or(ValidationError::OrderOrChain("segment_index_overflow"))?;
        let seal = self
            .last_seal
            .ok_or(ValidationError::OrderOrChain("segment_without_seal"))?;
        if segment_index != next || seal.final_segment {
            return Err(ValidationError::OrderOrChain("segment_transition"));
        }
        self.current_segment = segment_index;
        self.local_count = 0;
        self.local_bytes = 0;
        self.local_crc = Crc32::default();
        self.local_gap = false;
        Ok(())
    }

    pub(crate) fn accept(
        &mut self,
        frame: &RecordFrame,
        segment_index: usize,
        local_frame_start: u64,
        absolute_frame_start: u64,
        protected: &[u8],
        checksum: u32,
    ) -> Result<(), ValidationError> {
        if self.archive_sealed {
            return Err(ValidationError::TrailingData);
        }
        self.move_to_segment(segment_index)?;

        let segment_u32 = u32::try_from(segment_index)
            .map_err(|_| ValidationError::OrderOrChain("segment_index_overflow"))?;
        if frame.segment_no.get() != segment_u32 {
            return Err(ValidationError::OrderOrChain("segment_no"));
        }
        if local_frame_start != self.local_bytes {
            return Err(ValidationError::OrderOrChain("local_offset"));
        }
        let expected_record = match self.last_record {
            Some(last) => last.checked_next()?.get(),
            None => 1,
        };
        if frame.record_no.get() != expected_record {
            return Err(ValidationError::OrderOrChain("record_no"));
        }
        frame.validate_shape()?;

        if self.global_count == 0 {
            if segment_index != 0
                || local_frame_start != 0
                || !matches!(frame.value, Record::ArchiveStart(_))
            {
                return Err(ValidationError::OrderOrChain("archive_start_position"));
            }
        } else if self.local_count == 0 {
            if !matches!(frame.value, Record::SegmentStart(_)) {
                return Err(ValidationError::OrderOrChain("segment_start_position"));
            }
        } else if matches!(
            frame.value,
            Record::ArchiveStart(_) | Record::SegmentStart(_)
        ) {
            return Err(ValidationError::OrderOrChain("start_record_position"));
        }

        if let Some(seal) = self.last_seal
            && seal.segment.get() as usize == self.current_segment
        {
            if !seal.final_segment || !matches!(frame.value, Record::ArchiveSeal(_)) {
                return Err(ValidationError::OrderOrChain("record_after_segment_seal"));
            }
        }

        self.validate_value(frame, checksum, absolute_frame_start)?;

        let frame_len = protected
            .len()
            .checked_add(4)
            .ok_or(ValidationError::OrderOrChain("frame_length_overflow"))?;
        let wide_len = u64::try_from(frame_len)
            .map_err(|_| ValidationError::OrderOrChain("frame_length_overflow"))?;
        self.local_bytes = self
            .local_bytes
            .checked_add(wide_len)
            .ok_or(ValidationError::OrderOrChain("local_length_overflow"))?;
        self.local_count = self
            .local_count
            .checked_add(1)
            .ok_or(ValidationError::OrderOrChain("local_count_overflow"))?;
        self.global_count = self
            .global_count
            .checked_add(1)
            .ok_or(ValidationError::OrderOrChain("global_count_overflow"))?;
        self.local_crc.update(protected);
        self.global_crc.update(protected);
        self.last_record = Some(frame.record_no);
        self.last_kind = Some(frame.value.kind());
        if matches!(frame.value, Record::Gap(_)) {
            self.local_gap = true;
            self.any_gap = true;
        }
        Ok(())
    }

    fn validate_value(
        &mut self,
        frame: &RecordFrame,
        checksum: u32,
        absolute_frame_start: u64,
    ) -> Result<(), ValidationError> {
        match &frame.value {
            Record::ArchiveStart(value) => {
                if self.start.is_some() || absolute_frame_start != 0 {
                    return Err(ValidationError::OrderOrChain("duplicate_archive_start"));
                }
                self.mode = Some(value.mode);
                self.start = Some(value.clone());
            }
            Record::InstrumentSpec(value) => {
                self.check_context(&value.context)?;
                let version = value.numeric.fields().reference.version;
                let identity = value.numeric.fields().reference.instrument.clone();
                if self.specs.contains_key(&(value.slot, version)) {
                    return Err(ValidationError::Identity(IdentityError::IdentityConflict));
                }
                if self
                    .slot_identity
                    .get(&value.slot)
                    .is_some_and(|old| *old != identity)
                    || self
                        .identity_slot
                        .get(&identity)
                        .is_some_and(|old| *old != value.slot)
                {
                    return Err(ValidationError::Identity(IdentityError::IdentityConflict));
                }
                self.slot_identity.insert(value.slot, identity.clone());
                self.identity_slot.insert(identity, value.slot);
                self.specs.insert((value.slot, version), value.clone());
                self.active_spec.entry(value.slot).or_insert(version);
            }
            Record::StreamDefinition(value) => {
                self.check_context(&value.context)?;
                let version = value.binding.spec.version;
                let known = self
                    .specs
                    .get(&(value.binding.instrument_slot, version))
                    .ok_or(ValidationError::UnknownDefinition("InstrumentSpec"))?;
                if known.numeric.fields().reference != value.binding.spec {
                    return Err(ValidationError::Identity(IdentityError::SpecMismatch));
                }
                if self.active_spec.get(&value.binding.instrument_slot) != Some(&version) {
                    return Err(ValidationError::Identity(IdentityError::SpecMismatch));
                }
                value.binding.validate_registration(&self.bindings)?;
                self.bindings.push(value.binding.clone());
                self.streams.insert(
                    value.binding.id,
                    StreamState {
                        binding: value.binding.clone(),
                        loss: LossState::default(),
                    },
                );
            }
            Record::ConfigDefinition(value) => {
                self.check_context(&value.context)?;
                let mode = self
                    .mode
                    .ok_or(ValidationError::UnknownDefinition("ArchiveStart"))?;
                value.fields.validate(mode)?;
                if self.configs.contains(&value.next.config) {
                    return Err(ValidationError::Identity(IdentityError::IdentityConflict));
                }
                Self::check_open_windows(self.streams.values())?;
                self.configs.insert(value.next.config);
                self.active = Some(value.next);
            }
            Record::RawInput(value) => {
                self.check_context(&value.context)?;
                let stream = self
                    .streams
                    .get_mut(&value.stream)
                    .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                stream
                    .loss
                    .raw(value.attempt, value.tag)
                    .map_err(ValidationError::Loss)?;
            }
            Record::Control(value) => {
                self.check_context(&value.context)?;
                self.validate_control(frame.record_no, &value.value)?;
            }
            Record::Gap(value) => {
                self.check_context(&value.context)?;
                match &value.scope {
                    GapScope::AllDeclaredStreams => {
                        for stream in self.streams.values_mut() {
                            let target = GapTarget {
                                stream: stream.binding.id,
                                tag: stream.binding.tag,
                                range: None,
                                loss_count: None,
                            };
                            stream
                                .loss
                                .gap(&target, value.reason, stream.binding.tag)
                                .map_err(ValidationError::Loss)?;
                        }
                    }
                    GapScope::ExplicitTargets(targets) => {
                        for target in targets {
                            let stream = self
                                .streams
                                .get_mut(&target.stream)
                                .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                            stream
                                .loss
                                .gap(target, value.reason, stream.binding.tag)
                                .map_err(ValidationError::Loss)?;
                        }
                    }
                }
            }
            Record::SegmentSeal(value) => {
                if self.last_seal.is_some_and(|seal| {
                    seal.segment.get() as usize == self.current_segment
                }) {
                    return Err(ValidationError::OrderOrChain("duplicate_segment_seal"));
                }
                if value.prefix_frame_count != self.local_count
                    || value.prefix_physical_len != self.local_bytes
                    || value.prefix_crc32 != self.local_crc.digest()
                    || Some(value.prior_record) != self.last_record
                    || value.has_gap != self.local_gap
                {
                    return Err(ValidationError::OrderOrChain("segment_seal"));
                }
                self.last_seal = Some(SealLink {
                    segment: frame.segment_no,
                    record: frame.record_no,
                    checksum,
                    final_segment: value.is_final,
                });
            }
            Record::SegmentStart(value) => {
                let start = self
                    .start
                    .as_ref()
                    .ok_or(ValidationError::UnknownDefinition("ArchiveStart"))?;
                let seal = self
                    .last_seal
                    .ok_or(ValidationError::OrderOrChain("missing_previous_seal"))?;
                if seal.final_segment
                    || value.archive != start.archive
                    || value.session != start.session
                    || value.clock != start.clock
                    || value.previous_segment != seal.segment
                    || value.previous_seal_record != seal.record
                    || value.previous_seal_crc32 != seal.checksum
                    || seal.segment.get().checked_add(1) != Some(frame.segment_no.get())
                {
                    return Err(ValidationError::OrderOrChain("segment_start"));
                }
                self.last_seal = None;
            }
            Record::ArchiveSeal(value) => {
                let seal = self
                    .last_seal
                    .ok_or(ValidationError::OrderOrChain("missing_final_segment_seal"))?;
                let unresolved = self
                    .streams
                    .values()
                    .any(|stream| stream.loss.window.is_some());
                let quality_ok = match value.input_quality {
                    InputQuality::NoKnownLoss => !self.any_gap && !unresolved,
                    InputQuality::GapsRecorded => self.any_gap && !unresolved,
                    InputQuality::Unknown => true,
                };
                let expected_segments = frame
                    .segment_no
                    .get()
                    .checked_add(1)
                    .ok_or(ValidationError::OrderOrChain("segment_count_overflow"))?;
                if !seal.final_segment
                    || seal.segment != frame.segment_no
                    || Some(seal.record) != self.last_record
                    || value.expected_segment_count != expected_segments
                    || value.prior_frame_count != self.global_count
                    || value.total_prefix_physical_bytes != absolute_frame_start
                    || value.prefix_crc32 != self.global_crc.digest()
                    || Some(value.prior_record) != self.last_record
                    || !quality_ok
                {
                    return Err(ValidationError::OrderOrChain("archive_seal"));
                }
                self.archive_sealed = true;
                self.input_quality = Some(value.input_quality);
            }
        }
        Ok(())
    }

    fn validate_control(
        &mut self,
        record: RecordNo,
        control: &Control,
    ) -> Result<(), ValidationError> {
        match control {
            Control::Timer { stream, .. } => {
                if !self.streams.contains_key(stream) {
                    return Err(ValidationError::UnknownDefinition("StreamDefinition"));
                }
            }
            Control::Transport {
                connection,
                epoch,
                ..
            } => {
                let mut found = false;
                for stream in self.streams.values() {
                    if stream.binding.connection_id == *connection {
                        found = true;
                        if stream.binding.tag.connection != *epoch {
                            return Err(ValidationError::Identity(IdentityError::EpochMismatch));
                        }
                    }
                }
                if !found {
                    return Err(ValidationError::UnknownDefinition("ConnectionId"));
                }
            }
            Control::EpochAdvance { change, .. } => match change {
                EpochChange::Connection {
                    owner,
                    expected,
                    next,
                } => {
                    let affected: Vec<StreamId> = self
                        .streams
                        .iter()
                        .filter_map(|(id, stream)| {
                            (stream.binding.connection_id == *owner).then_some(*id)
                        })
                        .collect();
                    if affected.is_empty() {
                        return Err(ValidationError::UnknownDefinition("ConnectionId"));
                    }
                    Self::check_open_windows(
                        affected
                            .iter()
                            .filter_map(|id| self.streams.get(id)),
                    )?;
                    for id in affected {
                        let stream = self
                            .streams
                            .get_mut(&id)
                            .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                        if stream.binding.tag.connection != *expected || *next <= *expected {
                            return Err(ValidationError::Identity(IdentityError::EpochMismatch));
                        }
                        stream.binding.tag.connection = *next;
                    }
                }
                EpochChange::Subscription {
                    owner,
                    expected,
                    next,
                } => {
                    let stream = self
                        .streams
                        .get_mut(owner)
                        .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                    stream
                        .loss
                        .scope_change()
                        .map_err(ValidationError::Loss)?;
                    if stream.binding.tag.subscription != *expected || *next <= *expected {
                        return Err(ValidationError::Identity(IdentityError::EpochMismatch));
                    }
                    stream.binding.tag.subscription = *next;
                }
                EpochChange::Book {
                    owner,
                    expected,
                    next,
                } => {
                    let ids: Vec<StreamId> = self
                        .streams
                        .iter()
                        .filter_map(|(id, stream)| {
                            (stream.binding.book_id == Some(*owner)).then_some(*id)
                        })
                        .collect();
                    if ids.is_empty() {
                        return Err(ValidationError::UnknownDefinition("BookId"));
                    }
                    Self::check_open_windows(ids.iter().filter_map(|id| self.streams.get(id)))?;
                    for id in ids {
                        let stream = self
                            .streams
                            .get_mut(&id)
                            .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                        if stream.binding.tag.book != Some(*expected) || *next <= *expected {
                            return Err(ValidationError::Identity(IdentityError::EpochMismatch));
                        }
                        stream.binding.tag.book = Some(*next);
                    }
                }
            },
            Control::SpecActivate {
                slot,
                expected,
                next,
            } => {
                let current = self
                    .active_spec
                    .get(slot)
                    .copied()
                    .ok_or(ValidationError::UnknownDefinition("InstrumentSpec"))?;
                if current != *expected || *next <= *expected {
                    return Err(ValidationError::Identity(IdentityError::SpecMismatch));
                }
                let next_spec = self
                    .specs
                    .get(&(*slot, *next))
                    .ok_or(ValidationError::UnknownDefinition("InstrumentSpec"))?
                    .numeric
                    .fields()
                    .reference
                    .clone();
                let current_identity = self
                    .slot_identity
                    .get(slot)
                    .ok_or(ValidationError::UnknownDefinition("InstrumentSpec"))?;
                if next_spec.instrument != *current_identity {
                    return Err(ValidationError::Identity(IdentityError::IdentityMismatch));
                }
                let ids: Vec<StreamId> = self
                    .streams
                    .iter()
                    .filter_map(|(id, stream)| {
                        (stream.binding.instrument_slot == *slot).then_some(*id)
                    })
                    .collect();
                Self::check_open_windows(ids.iter().filter_map(|id| self.streams.get(id)))?;
                self.active_spec.insert(*slot, *next);
                for id in ids {
                    let stream = self
                        .streams
                        .get_mut(&id)
                        .ok_or(ValidationError::UnknownDefinition("StreamDefinition"))?;
                    stream.binding.spec = next_spec.clone();
                    stream.binding.tag.spec = *next;
                }
            }
            Control::Verification(value) => {
                if !self.streams.contains_key(&value.stream) {
                    return Err(ValidationError::UnknownDefinition("StreamDefinition"));
                }
                Self::check_record_reference(record, value.raw)?;
            }
            Control::Warmup(value) => {
                if !self.streams.contains_key(&value.stream) {
                    return Err(ValidationError::UnknownDefinition("StreamDefinition"));
                }
                Self::check_record_reference(record, value.anchor)?;
            }
            Control::Freshness(value) => {
                if !self.streams.contains_key(&value.stream) {
                    return Err(ValidationError::UnknownDefinition("StreamDefinition"));
                }
                if let Some(basis) = value.basis {
                    Self::check_record_reference(record, basis)?;
                }
            }
            Control::Recording(value) => {
                if value.health == RecordingHealth::Healthy && value.through.is_none() {
                    return Err(ValidationError::InvalidAcknowledgement);
                }
                if let Some(through) = value.through {
                    Self::check_record_reference(record, through)?;
                    self.watermarks.observe(value.kind, through)?;
                }
            }
        }
        Ok(())
    }
}

pub struct WalReader {
    paths: Vec<PathBuf>,
    files: Vec<File>,
    segment_index: usize,
    local_offset: u64,
    absolute_offset: u64,
    validator: ArchiveValidator,
    report: PhysicalReport,
    terminal: bool,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_segments(&[path.as_ref()])
    }

    pub fn open_segments<P: AsRef<Path>>(paths: &[P]) -> io::Result<Self> {
        if paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "at least one WAL segment path is required",
            ));
        }
        let mut owned_paths = Vec::with_capacity(paths.len());
        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let owned = path.as_ref().to_path_buf();
            files.push(File::open(&owned)?);
            owned_paths.push(owned);
        }
        Ok(Self {
            paths: owned_paths,
            files,
            segment_index: 0,
            local_offset: 0,
            absolute_offset: 0,
            validator: ArchiveValidator::default(),
            report: PhysicalReport::default(),
            terminal: false,
        })
    }

    pub fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    pub fn report(&self) -> &PhysicalReport {
        &self.report
    }

    pub fn next_record(&mut self) -> Result<Option<RecordFrame>, Failure> {
        if self.terminal {
            return Ok(None);
        }

        loop {
            let frame_start_local = self.local_offset;
            let frame_start_absolute = self.absolute_offset;
            let mut header_bytes = [0_u8; HEADER_LEN];
            let header_read = match read_up_to(
                &mut self.files[self.segment_index],
                &mut header_bytes,
            ) {
                Ok(count) => count,
                Err(error) => {
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Io {
                            operation: "read_header",
                            kind: error.kind(),
                        },
                        ArchiveStatus::Invalid,
                    );
                    return Err(failure);
                }
            };

            if header_read == 0 {
                if self.local_offset == 0 && self.segment_index > 0 {
                    let failure = self.fail(
                        0,
                        self.absolute_offset,
                        FailureKind::Validation(ValidationError::OrderOrChain(
                            "empty_segment",
                        )),
                        ArchiveStatus::Invalid,
                    );
                    return Err(failure);
                }
                if self.segment_index + 1 < self.files.len() {
                    if self.validator.archive_sealed() || !self.validator.can_rotate() {
                        let failure = self.fail(
                            self.local_offset,
                            self.absolute_offset,
                            FailureKind::Validation(if self.validator.archive_sealed() {
                                ValidationError::TrailingData
                            } else {
                                ValidationError::OrderOrChain("segment_without_nonfinal_seal")
                            }),
                            ArchiveStatus::Invalid,
                        );
                        return Err(failure);
                    }
                    self.segment_index += 1;
                    self.local_offset = 0;
                    continue;
                }
                self.report.status = self.validator.eof_status();
                self.report.input_quality = self.validator.input_quality();
                self.terminal = true;
                return Ok(None);
            }

            if header_read != HEADER_LEN {
                let failure = self.fail(
                    frame_start_local,
                    frame_start_absolute,
                    FailureKind::Codec(CodecError::new(
                        header_read,
                        CodecErrorKind::TruncatedTail,
                    )),
                    ArchiveStatus::TruncatedTail,
                );
                return Err(failure);
            }

            let header = match parse_header(&header_bytes, frame_start_absolute) {
                Ok(header) => header,
                Err(error) => {
                    let status = status_for_codec(&error);
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Codec(error),
                        status,
                    );
                    return Err(failure);
                }
            };

            let mut bytes = Vec::with_capacity(header.frame_len);
            bytes.extend_from_slice(&header_bytes);
            bytes.resize(header.frame_len, 0);
            let rest = &mut bytes[HEADER_LEN..];
            let rest_read = match read_up_to(&mut self.files[self.segment_index], rest) {
                Ok(count) => count,
                Err(error) => {
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Io {
                            operation: "read_frame",
                            kind: error.kind(),
                        },
                        ArchiveStatus::Invalid,
                    );
                    return Err(failure);
                }
            };
            if rest_read != rest.len() {
                let failure = self.fail(
                    frame_start_local,
                    frame_start_absolute,
                    FailureKind::Codec(CodecError::new(
                        HEADER_LEN + rest_read,
                        CodecErrorKind::TruncatedTail,
                    )),
                    ArchiveStatus::TruncatedTail,
                );
                return Err(failure);
            }

            let view = match scan_frame(&bytes, frame_start_absolute) {
                Ok(view) => view,
                Err(error) => {
                    let status = status_for_codec(&error);
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Codec(error),
                        status,
                    );
                    return Err(failure);
                }
            };
            let frame_len = u64::try_from(view.length()).map_err(|_| {
                self.fail(
                    frame_start_local,
                    frame_start_absolute,
                    FailureKind::Codec(CodecError::new(12, CodecErrorKind::LengthError)),
                    ArchiveStatus::Corrupt,
                )
            })?;
            let end = match frame_start_absolute.checked_add(frame_len) {
                Some(end) => end,
                None => {
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Codec(CodecError::new(12, CodecErrorKind::LengthError)),
                        ArchiveStatus::Corrupt,
                    );
                    return Err(failure);
                }
            };
            self.report.framing_good_offset = end;

            let frame = match decode_frame(
                &view,
                self.validator.has_active(),
                self.validator.definitions(),
            ) {
                Ok(frame) => frame,
                Err(error) => {
                    let status = status_for_codec(&error);
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Codec(error),
                        status,
                    );
                    return Err(failure);
                }
            };

            let protected_end = bytes.len() - 4;
            let mut candidate = self.validator.clone();
            if let Err(error) = candidate.accept(
                &frame,
                self.segment_index,
                frame_start_local,
                frame_start_absolute,
                &bytes[..protected_end],
                view.checksum,
            ) {
                let status = if error == ValidationError::TrailingData {
                    ArchiveStatus::Invalid
                } else {
                    ArchiveStatus::Invalid
                };
                let failure = self.fail(
                    frame_start_local,
                    frame_start_absolute,
                    FailureKind::Validation(error),
                    status,
                );
                return Err(failure);
            }
            self.validator = candidate;
            self.local_offset = match self.local_offset.checked_add(frame_len) {
                Some(offset) => offset,
                None => {
                    let failure = self.fail(
                        frame_start_local,
                        frame_start_absolute,
                        FailureKind::Validation(ValidationError::OrderOrChain(
                            "local_offset_overflow",
                        )),
                        ArchiveStatus::Invalid,
                    );
                    return Err(failure);
                }
            };
            self.absolute_offset = end;
            self.report.physical_good_offset = end;
            self.report.last_record = Some(frame.record_no);
            self.report.input_quality = self.validator.input_quality();
            self.report.status = self.validator.eof_status();
            return Ok(Some(frame));
        }
    }

    fn fail(
        &mut self,
        local_frame_start: u64,
        absolute_frame_start: u64,
        kind: FailureKind,
        status: ArchiveStatus,
    ) -> Failure {
        let failure = Failure {
            segment_index: self.segment_index,
            local_frame_start,
            absolute_frame_start,
            kind,
        };
        self.report.status = status;
        self.report.failure = Some(failure.clone());
        self.report.input_quality = self.validator.input_quality();
        self.terminal = true;
        failure
    }
}

fn status_for_codec(error: &CodecError) -> ArchiveStatus {
    match error.kind {
        CodecErrorKind::TruncatedTail => ArchiveStatus::TruncatedTail,
        CodecErrorKind::Unsupported { .. } => ArchiveStatus::Unsupported,
        CodecErrorKind::ChecksumMismatch
        | CodecErrorKind::Corrupt(_)
        | CodecErrorKind::InvalidPayload(_)
        | CodecErrorKind::LengthError
        | CodecErrorKind::Identity(_)
        | CodecErrorKind::Numeric(_)
        | CodecErrorKind::Value(_)
        | CodecErrorKind::Artifact(_)
        | CodecErrorKind::Policy(_)
        | CodecErrorKind::Event(_)
        | CodecErrorKind::Record(_)
        | CodecErrorKind::UnknownDefinition => ArchiveStatus::Corrupt,
    }
}

fn read_up_to(reader: &mut File, bytes: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < bytes.len() {
        match reader.read(&mut bytes[total..]) {
            Ok(0) => break,
            Ok(count) => {
                total = total.checked_add(count).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "read length overflow")
                })?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

pub fn read_all<P: AsRef<Path>>(paths: &[P]) -> io::Result<ReadArchive> {
    let mut reader = WalReader::open_segments(paths)?;
    let mut records = Vec::new();
    loop {
        match reader.next_record() {
            Ok(Some(record)) => records.push(record),
            Ok(None) | Err(_) => break,
        }
    }
    Ok(ReadArchive {
        records,
        report: reader.report.clone(),
    })
}
