use std::collections::BTreeMap;

use domain::identity::*;
use domain::record::*;

use crate::binary::{CodecError, CodecErrorKind, Reader, Result, Writer, checked, crc32};

pub const MAX_PAYLOAD: usize = 1_048_576;
pub const HEADER_LEN: usize = 32;
pub const MAX_FRAME_LEN: usize = MAX_PAYLOAD + HEADER_LEN + 4;

pub type Definitions = BTreeMap<(InstrumentSlot, SpecVersion), InstrumentSpecRecord>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub kind: RecordKind,
    pub record_no: RecordNo,
    pub segment_no: SegmentNo,
    pub payload_len: usize,
    pub frame_len: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameView<'a> {
    pub header: FrameHeader,
    pub payload: &'a [u8],
    pub checksum: u32,
}

impl<'a> FrameView<'a> {
    pub const fn length(&self) -> usize {
        self.header.frame_len
    }
}

pub(crate) fn parse_header(bytes: &[u8; HEADER_LEN], absolute_offset: u64) -> Result<FrameHeader> {
    let mut reader = Reader::new(bytes, 0)?;
    if reader.take(4)? != b"PSRW" {
        return Err(CodecError::new(0, CodecErrorKind::Corrupt("magic")));
    }
    for field in ["frame_version", "record_schema_version"] {
        let offset = reader.offset();
        let value = reader.u16()?;
        if value != 1 {
            return Err(CodecError::new(
                offset,
                CodecErrorKind::Unsupported {
                    field,
                    value: value.into(),
                },
            ));
        }
    }
    let tag = reader.u16()?;
    let kind = RecordKind::try_from(tag).map_err(|_| {
        CodecError::new(
            8,
            CodecErrorKind::Unsupported {
                field: "record_kind",
                value: tag.into(),
            },
        )
    })?;
    if reader.u16()? != 0 {
        return Err(CodecError::new(10, CodecErrorKind::Corrupt("flags")));
    }
    let payload_len = usize::try_from(reader.u32()?)
        .map_err(|_| CodecError::new(12, CodecErrorKind::LengthError))?;
    if payload_len > MAX_PAYLOAD {
        return Err(CodecError::new(12, CodecErrorKind::LengthError));
    }
    let record_no = reader.record_no()?;
    let segment_no = SegmentNo::new(reader.u32()?);
    if reader.u32()? != 0 {
        return Err(CodecError::new(28, CodecErrorKind::Corrupt("reserved")));
    }
    let frame_len = payload_len
        .checked_add(HEADER_LEN + 4)
        .ok_or_else(|| CodecError::new(12, CodecErrorKind::LengthError))?;
    let wide_len =
        u64::try_from(frame_len).map_err(|_| CodecError::new(12, CodecErrorKind::LengthError))?;
    absolute_offset
        .checked_add(wide_len)
        .ok_or_else(|| CodecError::new(12, CodecErrorKind::LengthError))?;
    Ok(FrameHeader {
        kind,
        record_no,
        segment_no,
        payload_len,
        frame_len,
    })
}

/// Scan exactly one frame at the front of a borrowed slice.
///
/// A suffix is deliberately untouched. Recovery never searches for a later
/// magic value after a malformed frame.
pub fn scan_frame(bytes: &[u8], absolute_offset: u64) -> Result<FrameView<'_>> {
    if bytes.len() < HEADER_LEN {
        return Err(CodecError::new(0, CodecErrorKind::TruncatedTail));
    }
    let mut header_bytes = [0_u8; HEADER_LEN];
    header_bytes.copy_from_slice(&bytes[..HEADER_LEN]);
    let header = parse_header(&header_bytes, absolute_offset)?;
    if bytes.len() < header.frame_len {
        return Err(CodecError::new(0, CodecErrorKind::TruncatedTail));
    }
    let protected_end = header.frame_len - 4;
    let mut trailer = Reader::new(&bytes[protected_end..header.frame_len], protected_end)?;
    let checksum = trailer.u32()?;
    if crc32(&bytes[..protected_end]) != checksum {
        return Err(CodecError::new(
            protected_end,
            CodecErrorKind::ChecksumMismatch,
        ));
    }
    Ok(FrameView {
        header,
        payload: &bytes[HEADER_LEN..protected_end],
        checksum,
    })
}

pub fn decode_frame(
    view: &FrameView<'_>,
    has_active: bool,
    specs: &Definitions,
) -> Result<RecordFrame> {
    let mut reader = Reader::new(view.payload, HEADER_LEN)?;
    let value = match view.header.kind {
        RecordKind::ArchiveStart => Record::ArchiveStart(ArchiveStart {
            archive: reader.archive()?,
            session: reader.session()?,
            clock: reader.clock()?,
            mode: reader.enum_tag("ArchiveStart.durability_mode")?,
            previous_archive: reader.option(Reader::archive)?,
        }),
        RecordKind::InstrumentSpec => {
            let context = reader.context(2, has_active)?;
            let (slot, numeric) = reader.numeric_spec()?;
            Record::InstrumentSpec(InstrumentSpecRecord {
                context,
                slot,
                numeric,
                provenance: reader.artifact()?,
            })
        }
        RecordKind::StreamDefinition => {
            let context = reader.context(3, has_active)?;
            let id = reader.stream()?;
            let instrument_slot = reader.slot()?;
            let spec_offset = reader.offset();
            let version = reader.spec_version()?;
            let known = specs
                .get(&(instrument_slot, version))
                .ok_or_else(|| CodecError::new(spec_offset, CodecErrorKind::UnknownDefinition))?;
            let connection_id = reader.connection()?;
            let connection = reader.connection_epoch()?;
            let subscription = reader.subscription_epoch()?;
            let channel = reader.enum_tag("Channel")?;
            let book_id = reader.option(Reader::book)?;
            let book = reader.option(Reader::book_epoch)?;
            let feed_profile = reader.profile_version()?;
            let binding = StreamBinding {
                id,
                instrument_slot,
                spec: known.numeric.fields().reference.clone(),
                connection_id,
                channel,
                book_id,
                tag: EpochTag {
                    spec: version,
                    connection,
                    subscription,
                    book,
                },
                feed_profile,
            };
            Record::StreamDefinition(StreamDefinition {
                context,
                binding,
                provenance: reader.artifact()?,
            })
        }
        RecordKind::ConfigDefinition => Record::ConfigDefinition(ConfigDefinition {
            context: reader.context(4, has_active)?,
            next: reader.active_context()?,
            provenance_kind: reader.enum_tag("provenance_kind")?,
            evidence: reader.artifact()?,
            fields: reader.policy_fields()?,
        }),
        RecordKind::RawInput => {
            let context = reader.context(5, has_active)?;
            let stream = reader.stream()?;
            let tag = reader.epoch_tag()?;
            let attempt = reader.attempt()?;
            let offset = reader.offset();
            let encoding = reader.u8()?;
            if encoding != 1 {
                return Err(CodecError::new(
                    offset,
                    CodecErrorKind::Unsupported {
                        field: "RawInput.payload_encoding",
                        value: encoding.into(),
                    },
                ));
            }
            let offset = reader.offset();
            let length = usize::try_from(reader.u32()?)
                .map_err(|_| CodecError::new(offset, CodecErrorKind::LengthError))?;
            if length != reader.remaining() || length > MAX_PAYLOAD {
                return Err(CodecError::new(offset, CodecErrorKind::LengthError));
            }
            let bytes = reader.take(length)?.to_vec();
            Record::RawInput(RawInput {
                context,
                stream,
                tag,
                attempt,
                bytes,
            })
        }
        RecordKind::Control => {
            let context = reader.context(6, has_active)?;
            Record::Control(ControlRecord {
                context,
                value: decode_control(&mut reader)?,
            })
        }
        RecordKind::Gap => {
            let context = reader.context(7, has_active)?;
            Record::Gap(decode_gap(&mut reader, context)?)
        }
        RecordKind::SegmentSeal => Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: reader.u64()?,
            prefix_physical_len: reader.u64()?,
            prefix_crc32: reader.u32()?,
            prior_record: reader.record_no()?,
            has_gap: reader.bool()?,
            is_final: reader.bool()?,
        }),
        RecordKind::SegmentStart => Record::SegmentStart(SegmentStart {
            archive: reader.archive()?,
            session: reader.session()?,
            clock: reader.clock()?,
            previous_segment: SegmentNo::new(reader.u32()?),
            previous_seal_record: reader.record_no()?,
            previous_seal_crc32: reader.u32()?,
        }),
        RecordKind::ArchiveSeal => Record::ArchiveSeal(ArchiveSeal {
            expected_segment_count: reader.u32()?,
            prior_frame_count: reader.u64()?,
            total_prefix_physical_bytes: reader.u64()?,
            prefix_crc32: reader.u32()?,
            prior_record: reader.record_no()?,
            input_quality: reader.enum_tag("ArchiveSeal.input_quality")?,
        }),
    };
    reader.finish()?;
    let frame = RecordFrame {
        record_no: view.header.record_no,
        segment_no: view.header.segment_no,
        value,
    };
    checked(HEADER_LEN, frame.validate_shape())?;
    Ok(frame)
}

pub fn decode_exact(bytes: &[u8], has_active: bool, specs: &Definitions) -> Result<RecordFrame> {
    let view = scan_frame(bytes, 0)?;
    if view.length() != bytes.len() {
        return Err(CodecError::new(
            view.length(),
            CodecErrorKind::InvalidPayload("trailing_bytes"),
        ));
    }
    decode_frame(&view, has_active, specs)
}

pub fn encode_frame(frame: &RecordFrame) -> Result<Vec<u8>> {
    checked(HEADER_LEN, frame.validate_shape())?;
    let mut writer = Writer::new(MAX_PAYLOAD);
    if let Some(context) = frame.value.context() {
        writer.context(*context)?;
    }
    match &frame.value {
        Record::ArchiveStart(value) => {
            writer.bytes(value.archive.as_bytes())?;
            writer.bytes(value.session.as_bytes())?;
            writer.u32(value.clock.get())?;
            writer.u8(value.mode.tag())?;
            writer.option(value.previous_archive, |writer, id| {
                writer.bytes(id.as_bytes())
            })?;
        }
        Record::InstrumentSpec(value) => {
            writer.numeric_spec(value.slot, &value.numeric)?;
            writer.artifact(value.provenance)?;
        }
        Record::StreamDefinition(value) => {
            let binding = &value.binding;
            writer.u32(binding.id.get())?;
            writer.u32(binding.instrument_slot.get())?;
            writer.u32(binding.spec.version.get())?;
            writer.u32(binding.connection_id.get())?;
            writer.u64(binding.tag.connection.get())?;
            writer.u64(binding.tag.subscription.get())?;
            writer.u8(binding.channel.tag())?;
            writer.option(binding.book_id, |writer, id| writer.u32(id.get()))?;
            writer.option(binding.tag.book, |writer, epoch| writer.u64(epoch.get()))?;
            writer.u32(binding.feed_profile.get())?;
            writer.artifact(value.provenance)?;
        }
        Record::ConfigDefinition(value) => {
            writer.active_context(value.next)?;
            writer.u8(value.provenance_kind.tag())?;
            writer.artifact(value.evidence)?;
            writer.policy_fields(value.fields)?;
        }
        Record::RawInput(value) => {
            writer.u32(value.stream.get())?;
            writer.epoch_tag(value.tag)?;
            writer.u64(value.attempt.get())?;
            writer.u8(1)?;
            let length = u32::try_from(value.bytes.len())
                .map_err(|_| CodecError::new(12, CodecErrorKind::LengthError))?;
            writer.u32(length)?;
            writer.bytes(&value.bytes)?;
        }
        Record::Control(value) => encode_control(&mut writer, &value.value)?,
        Record::Gap(value) => encode_gap(&mut writer, value)?,
        Record::SegmentSeal(value) => {
            writer.u64(value.prefix_frame_count)?;
            writer.u64(value.prefix_physical_len)?;
            writer.u32(value.prefix_crc32)?;
            writer.u64(value.prior_record.get())?;
            writer.bool(value.has_gap)?;
            writer.bool(value.is_final)?;
        }
        Record::SegmentStart(value) => {
            writer.bytes(value.archive.as_bytes())?;
            writer.bytes(value.session.as_bytes())?;
            writer.u32(value.clock.get())?;
            writer.u32(value.previous_segment.get())?;
            writer.u64(value.previous_seal_record.get())?;
            writer.u32(value.previous_seal_crc32)?;
        }
        Record::ArchiveSeal(value) => {
            writer.u32(value.expected_segment_count)?;
            writer.u64(value.prior_frame_count)?;
            writer.u64(value.total_prefix_physical_bytes)?;
            writer.u32(value.prefix_crc32)?;
            writer.u64(value.prior_record.get())?;
            writer.u8(value.input_quality.tag())?;
        }
    }
    let payload = writer.finish();
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| CodecError::new(12, CodecErrorKind::LengthError))?;
    let mut writer = Writer::new(MAX_FRAME_LEN);
    writer.bytes(b"PSRW")?;
    writer.u16(1)?;
    writer.u16(1)?;
    writer.u16(frame.value.kind().tag())?;
    writer.u16(0)?;
    writer.u32(payload_len)?;
    writer.u64(frame.record_no.get())?;
    writer.u32(frame.segment_no.get())?;
    writer.u32(0)?;
    writer.bytes(&payload)?;
    let mut bytes = writer.finish();
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn decode_control(reader: &mut Reader<'_>) -> Result<Control> {
    let offset = reader.offset();
    let tag = reader.u8()?;
    Ok(match tag {
        1 => Control::Timer {
            stream: reader.stream()?,
            timer_id: reader.u64()?,
            deadline_ns: reader.u64()?,
        },
        2 => Control::Transport {
            connection: reader.connection()?,
            epoch: reader.connection_epoch()?,
            value: reader.enum_tag("Transport.liveness")?,
        },
        3 => {
            let offset = reader.offset();
            let scope = reader.u8()?;
            let change = match scope {
                1 => EpochChange::Connection {
                    owner: reader.connection()?,
                    expected: reader.connection_epoch()?,
                    next: reader.connection_epoch()?,
                },
                2 => EpochChange::Subscription {
                    owner: reader.stream()?,
                    expected: reader.subscription_epoch()?,
                    next: reader.subscription_epoch()?,
                },
                3 => EpochChange::Book {
                    owner: reader.book()?,
                    expected: reader.book_epoch()?,
                    next: reader.book_epoch()?,
                },
                _ => {
                    return Err(CodecError::new(
                        offset,
                        CodecErrorKind::Unsupported {
                            field: "EpochAdvance.scope",
                            value: scope.into(),
                        },
                    ));
                }
            };
            Control::EpochAdvance {
                change,
                reason: reader.enum_tag("reason")?,
            }
        }
        4 => Control::SpecActivate {
            slot: reader.slot()?,
            expected: reader.spec_version()?,
            next: reader.spec_version()?,
        },
        5 => Control::Verification(VerificationEvidence {
            stream: reader.stream()?,
            tag: reader.epoch_tag()?,
            raw: reader.record_no()?,
            kind: reader.enum_tag("Verification.evidence_kind")?,
            profile: reader.profile_version()?,
            proof: reader.artifact()?,
        }),
        6 => Control::Warmup(WarmupEvidence {
            stream: reader.stream()?,
            tag: reader.epoch_tag()?,
            anchor: reader.record_no()?,
            update_count: reader.u32()?,
            elapsed_ns: reader.u64()?,
            proof: reader.artifact()?,
        }),
        7 => Control::Freshness(FreshnessEvidence {
            stream: reader.stream()?,
            tag: reader.epoch_tag()?,
            freshness: reader.enum_tag("FreshnessEvidence.freshness")?,
            basis: reader.option(Reader::record_no)?,
            proof: reader.artifact()?,
        }),
        8 => Control::Recording(RecordingEvidence {
            health: reader.enum_tag("RecordingEvidence.health")?,
            kind: reader.enum_tag("RecordingEvidence.watermark_kind")?,
            through: reader.option(Reader::record_no)?,
            reason: reader.enum_tag("reason")?,
        }),
        _ => {
            return Err(CodecError::new(
                offset,
                CodecErrorKind::Unsupported {
                    field: "control_tag",
                    value: tag.into(),
                },
            ));
        }
    })
}

fn decode_gap(reader: &mut Reader<'_>, context: WireContext) -> Result<Gap> {
    let scope_offset = reader.offset();
    let scope_tag = reader.u8()?;
    if scope_tag != 1 && scope_tag != 2 {
        return Err(CodecError::new(
            scope_offset,
            CodecErrorKind::Unsupported {
                field: "Gap.scope_kind",
                value: scope_tag.into(),
            },
        ));
    }
    let reason = reader.enum_tag("reason")?;
    let count_offset = reader.offset();
    let count = usize::from(reader.u16()?);
    let scope = if scope_tag == 2 {
        if count != 0 {
            return Err(CodecError::new(
                count_offset,
                CodecErrorKind::InvalidPayload("Gap.target_count"),
            ));
        }
        GapScope::AllDeclaredStreams
    } else {
        if count == 0 || count > 256 {
            return Err(CodecError::new(
                count_offset,
                CodecErrorKind::InvalidPayload("Gap.target_count"),
            ));
        }
        reader.count(count, 28, 256)?;
        let mut targets = Vec::with_capacity(count);
        for _ in 0..count {
            let stream = reader.stream()?;
            let tag = reader.epoch_tag()?;
            let offset = reader.offset();
            let first = reader.option(Reader::attempt)?;
            let last = reader.option(Reader::attempt)?;
            let range = match (first, last) {
                (None, None) => None,
                (Some(first), Some(last)) => Some((first, last)),
                _ => {
                    return Err(CodecError::new(
                        offset,
                        CodecErrorKind::InvalidPayload("Gap.range"),
                    ));
                }
            };
            let loss_count = reader.option(Reader::u64)?;
            targets.push(GapTarget {
                stream,
                tag,
                range,
                loss_count,
            });
        }
        GapScope::ExplicitTargets(targets)
    };
    let gap = Gap {
        context,
        scope,
        reason,
    };
    checked(scope_offset, gap.validate())?;
    Ok(gap)
}

fn encode_control(writer: &mut Writer, value: &Control) -> Result<()> {
    writer.u8(value.tag())?;
    match value {
        Control::Timer {
            stream,
            timer_id,
            deadline_ns,
        } => {
            writer.u32(stream.get())?;
            writer.u64(*timer_id)?;
            writer.u64(*deadline_ns)?;
        }
        Control::Transport {
            connection,
            epoch,
            value,
        } => {
            writer.u32(connection.get())?;
            writer.u64(epoch.get())?;
            writer.u8(value.tag())?;
        }
        Control::EpochAdvance { change, reason } => {
            let (scope, owner, expected, next) = change.wire_parts();
            writer.u8(scope)?;
            writer.u32(owner)?;
            writer.u64(expected)?;
            writer.u64(next)?;
            writer.u8(reason.tag())?;
        }
        Control::SpecActivate {
            slot,
            expected,
            next,
        } => {
            writer.u32(slot.get())?;
            writer.u32(expected.get())?;
            writer.u32(next.get())?;
        }
        Control::Verification(value) => {
            writer.u32(value.stream.get())?;
            writer.epoch_tag(value.tag)?;
            writer.u64(value.raw.get())?;
            writer.u8(value.kind.tag())?;
            writer.u32(value.profile.get())?;
            writer.artifact(value.proof)?;
        }
        Control::Warmup(value) => {
            writer.u32(value.stream.get())?;
            writer.epoch_tag(value.tag)?;
            writer.u64(value.anchor.get())?;
            writer.u32(value.update_count)?;
            writer.u64(value.elapsed_ns)?;
            writer.artifact(value.proof)?;
        }
        Control::Freshness(value) => {
            writer.u32(value.stream.get())?;
            writer.epoch_tag(value.tag)?;
            writer.u8(value.freshness.tag())?;
            writer.option(value.basis, |writer, record| writer.u64(record.get()))?;
            writer.artifact(value.proof)?;
        }
        Control::Recording(value) => {
            writer.u8(value.health.tag())?;
            writer.u8(value.kind.tag())?;
            writer.option(value.through, |writer, record| writer.u64(record.get()))?;
            writer.u8(value.reason.tag())?;
        }
    }
    Ok(())
}

fn encode_gap(writer: &mut Writer, value: &Gap) -> Result<()> {
    writer.u8(value.scope.tag())?;
    writer.u8(value.reason.tag())?;
    match &value.scope {
        GapScope::AllDeclaredStreams => writer.u16(0)?,
        GapScope::ExplicitTargets(targets) => {
            let count = u16::try_from(targets.len())
                .map_err(|_| CodecError::new(58, CodecErrorKind::LengthError))?;
            writer.u16(count)?;
            for target in targets {
                writer.u32(target.stream.get())?;
                writer.epoch_tag(target.tag)?;
                writer.option(target.range.map(|value| value.0), |writer, value| {
                    writer.u64(value.get())
                })?;
                writer.option(target.range.map(|value| value.1), |writer, value| {
                    writer.u64(value.get())
                })?;
                writer.option(target.loss_count, Writer::u64)?;
            }
        }
    }
    Ok(())
}
