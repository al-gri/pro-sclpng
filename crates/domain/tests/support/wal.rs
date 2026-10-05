//! Memory-only WAL v1 framing and payload codec. Compiled only in tests.
//! Header/payload bytes are explicit; no native layout or filesystem access.

use std::collections::BTreeMap;

use domain::identity::*;
use domain::record::*;

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked, crc32};
use super::wal_control::{decode_control, decode_gap, encode_control, encode_gap};

pub const MAX_PAYLOAD: usize = 1_048_576;
pub const HEADER_LEN: usize = 32;
pub type Definitions = BTreeMap<(InstrumentSlot, SpecVersion), InstrumentSpecRecord>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameView<'a> {
    pub kind: RecordKind,
    pub record_no: RecordNo,
    pub segment_no: SegmentNo,
    pub payload: &'a [u8],
    pub checksum: u32,
    pub length: usize,
}

/// Scan exactly one frame at the front of a borrowed slice. A suffix is left
/// untouched for the recovery scanner, never searched for another magic value.
pub fn scan_frame(bytes: &[u8], absolute_offset: u64) -> Result<FrameView<'_>> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::new(0, ErrorKind::TruncatedTail));
    }
    let mut r = Reader::new(&bytes[..HEADER_LEN], 0)?;
    if r.take(4)? != b"PSRW" {
        return Err(Error::new(0, ErrorKind::Corrupt("magic")));
    }
    for field in ["frame_version", "record_schema_version"] {
        let offset = r.offset();
        let value = r.u16()?;
        if value != 1 {
            return Err(Error::new(offset, ErrorKind::Unsupported { field, value: value.into() }));
        }
    }
    let tag = r.u16()?;
    let kind = RecordKind::try_from(tag).map_err(|_| {
        Error::new(8, ErrorKind::Unsupported { field: "record_kind", value: tag.into() })
    })?;
    if r.u16()? != 0 {
        return Err(Error::new(10, ErrorKind::Corrupt("flags")));
    }
    let payload_len = usize::try_from(r.u32()?)
        .map_err(|_| Error::new(12, ErrorKind::LengthError))?;
    if payload_len > MAX_PAYLOAD {
        return Err(Error::new(12, ErrorKind::LengthError));
    }
    let record_no = r.record_no()?;
    let segment_no = SegmentNo::new(r.u32()?);
    if r.u32()? != 0 {
        return Err(Error::new(28, ErrorKind::Corrupt("reserved")));
    }
    let length = payload_len.checked_add(36)
        .ok_or_else(|| Error::new(12, ErrorKind::LengthError))?;
    let wide_length = u64::try_from(length)
        .map_err(|_| Error::new(12, ErrorKind::LengthError))?;
    absolute_offset.checked_add(wide_length)
        .ok_or_else(|| Error::new(12, ErrorKind::LengthError))?;
    if bytes.len() < length {
        return Err(Error::new(0, ErrorKind::TruncatedTail));
    }
    let protected_end = length - 4;
    let mut trailer = Reader::new(&bytes[protected_end..length], protected_end)?;
    let checksum = trailer.u32()?;
    if crc32(&bytes[..protected_end]) != checksum {
        return Err(Error::new(protected_end, ErrorKind::ChecksumMismatch));
    }
    Ok(FrameView {
        kind,
        record_no,
        segment_no,
        payload: &bytes[HEADER_LEN..protected_end],
        checksum,
        length,
    })
}

pub fn decode_frame(view: &FrameView<'_>, has_active: bool, specs: &Definitions) -> Result<RecordFrame> {
    let mut r = Reader::new(view.payload, HEADER_LEN)?;
    let value = match view.kind {
        RecordKind::ArchiveStart => Record::ArchiveStart(ArchiveStart {
            archive: r.archive()?,
            session: r.session()?,
            clock: r.clock()?,
            mode: r.enum_tag("ArchiveStart.durability_mode")?,
            previous_archive: r.option(Reader::archive)?,
        }),
        RecordKind::InstrumentSpec => {
            let context = r.context(2, has_active)?;
            let (slot, numeric) = r.numeric_spec()?;
            Record::InstrumentSpec(InstrumentSpecRecord {
                context,
                slot,
                numeric,
                provenance: r.artifact()?,
            })
        }
        RecordKind::StreamDefinition => {
            let context = r.context(3, has_active)?;
            let id = r.stream()?;
            let instrument_slot = r.slot()?;
            let spec_offset = r.offset();
            let version = r.spec_version()?;
            let known = specs.get(&(instrument_slot, version))
                .ok_or_else(|| Error::new(spec_offset, ErrorKind::UnknownDefinition))?;
            let connection_id = r.connection()?;
            let connection = r.connection_epoch()?;
            let subscription = r.subscription_epoch()?;
            let channel = r.enum_tag("Channel")?;
            let book_id = r.option(Reader::book)?;
            let book = r.option(Reader::book_epoch)?;
            let feed_profile = r.profile_version()?;
            let binding = StreamBinding {
                id,
                instrument_slot,
                spec: known.numeric.fields().reference.clone(),
                connection_id,
                channel,
                book_id,
                tag: EpochTag { spec: version, connection, subscription, book },
                feed_profile,
            };
            Record::StreamDefinition(StreamDefinition {
                context,
                binding,
                provenance: r.artifact()?,
            })
        }
        RecordKind::ConfigDefinition => Record::ConfigDefinition(ConfigDefinition {
            context: r.context(4, has_active)?,
            next: r.active_context()?,
            provenance_kind: r.enum_tag("provenance_kind")?,
            evidence: r.artifact()?,
            fields: r.policy_fields()?,
        }),
        RecordKind::RawInput => {
            let context = r.context(5, has_active)?;
            let stream = r.stream()?;
            let tag = r.epoch_tag()?;
            let attempt = r.attempt()?;
            let offset = r.offset();
            let encoding = r.u8()?;
            if encoding != 1 {
                return Err(Error::new(offset, ErrorKind::Unsupported {
                    field: "RawInput.payload_encoding", value: encoding.into(),
                }));
            }
            let offset = r.offset();
            let length = usize::try_from(r.u32()?)
                .map_err(|_| Error::new(offset, ErrorKind::LengthError))?;
            if length != r.remaining() || length > MAX_PAYLOAD {
                return Err(Error::new(offset, ErrorKind::LengthError));
            }
            // Length has already been compared to the validated frame slice.
            let bytes = r.take(length)?.to_vec();
            Record::RawInput(RawInput { context, stream, tag, attempt, bytes })
        }
        RecordKind::Control => {
            let context = r.context(6, has_active)?;
            Record::Control(ControlRecord { context, value: decode_control(&mut r)? })
        }
        RecordKind::Gap => {
            let context = r.context(7, has_active)?;
            Record::Gap(decode_gap(&mut r, context)?)
        }
        RecordKind::SegmentSeal => Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: r.u64()?,
            prefix_physical_len: r.u64()?,
            prefix_crc32: r.u32()?,
            prior_record: r.record_no()?,
            has_gap: r.bool()?,
            is_final: r.bool()?,
        }),
        RecordKind::SegmentStart => Record::SegmentStart(SegmentStart {
            archive: r.archive()?,
            session: r.session()?,
            clock: r.clock()?,
            previous_segment: SegmentNo::new(r.u32()?),
            previous_seal_record: r.record_no()?,
            previous_seal_crc32: r.u32()?,
        }),
        RecordKind::ArchiveSeal => Record::ArchiveSeal(ArchiveSeal {
            expected_segment_count: r.u32()?,
            prior_frame_count: r.u64()?,
            total_prefix_physical_bytes: r.u64()?,
            prefix_crc32: r.u32()?,
            prior_record: r.record_no()?,
            input_quality: r.enum_tag("ArchiveSeal.input_quality")?,
        }),
    };
    r.finish()?;
    let frame = RecordFrame { record_no: view.record_no, segment_no: view.segment_no, value };
    checked(HEADER_LEN, frame.validate_shape())?;
    Ok(frame)
}

pub fn decode_exact(bytes: &[u8], has_active: bool, specs: &Definitions) -> Result<RecordFrame> {
    let view = scan_frame(bytes, 0)?;
    if view.length != bytes.len() {
        return Err(Error::new(view.length, ErrorKind::InvalidPayload("trailing_bytes")));
    }
    decode_frame(&view, has_active, specs)
}

pub fn encode_frame(frame: &RecordFrame) -> Result<Vec<u8>> {
    checked(HEADER_LEN, frame.validate_shape())?;
    let mut w = Writer::new(MAX_PAYLOAD);
    if let Some(context) = frame.value.context() {
        w.context(*context)?;
    }
    match &frame.value {
        Record::ArchiveStart(v) => {
            w.bytes(v.archive.as_bytes())?;
            w.bytes(v.session.as_bytes())?;
            w.u32(v.clock.get())?;
            w.u8(v.mode.tag())?;
            w.option(v.previous_archive, |w, id| w.bytes(id.as_bytes()))?;
        }
        Record::InstrumentSpec(v) => {
            w.numeric_spec(v.slot, &v.numeric)?;
            w.artifact(v.provenance)?;
        }
        Record::StreamDefinition(v) => {
            let b = &v.binding;
            w.u32(b.id.get())?;
            w.u32(b.instrument_slot.get())?;
            w.u32(b.spec.version.get())?;
            w.u32(b.connection_id.get())?;
            w.u64(b.tag.connection.get())?;
            w.u64(b.tag.subscription.get())?;
            w.u8(b.channel.tag())?;
            w.option(b.book_id, |w, id| w.u32(id.get()))?;
            w.option(b.tag.book, |w, epoch| w.u64(epoch.get()))?;
            w.u32(b.feed_profile.get())?;
            w.artifact(v.provenance)?;
        }
        Record::ConfigDefinition(v) => {
            w.active_context(v.next)?;
            w.u8(v.provenance_kind.tag())?;
            w.artifact(v.evidence)?;
            w.policy_fields(v.fields)?;
        }
        Record::RawInput(v) => {
            w.u32(v.stream.get())?;
            w.epoch_tag(v.tag)?;
            w.u64(v.attempt.get())?;
            w.u8(1)?;
            let length = u32::try_from(v.bytes.len())
                .map_err(|_| Error::new(12, ErrorKind::LengthError))?;
            w.u32(length)?;
            w.bytes(&v.bytes)?;
        }
        Record::Control(v) => encode_control(&mut w, &v.value)?,
        Record::Gap(v) => encode_gap(&mut w, v)?,
        Record::SegmentSeal(v) => {
            w.u64(v.prefix_frame_count)?;
            w.u64(v.prefix_physical_len)?;
            w.u32(v.prefix_crc32)?;
            w.u64(v.prior_record.get())?;
            w.bool(v.has_gap)?;
            w.bool(v.is_final)?;
        }
        Record::SegmentStart(v) => {
            w.bytes(v.archive.as_bytes())?;
            w.bytes(v.session.as_bytes())?;
            w.u32(v.clock.get())?;
            w.u32(v.previous_segment.get())?;
            w.u64(v.previous_seal_record.get())?;
            w.u32(v.previous_seal_crc32)?;
        }
        Record::ArchiveSeal(v) => {
            w.u32(v.expected_segment_count)?;
            w.u64(v.prior_frame_count)?;
            w.u64(v.total_prefix_physical_bytes)?;
            w.u32(v.prefix_crc32)?;
            w.u64(v.prior_record.get())?;
            w.u8(v.input_quality.tag())?;
        }
    }
    let payload = w.finish();
    let length = u32::try_from(payload.len())
        .map_err(|_| Error::new(12, ErrorKind::LengthError))?;
    let mut w = Writer::new(MAX_PAYLOAD + 36);
    w.bytes(b"PSRW")?;
    w.u16(1)?;
    w.u16(1)?;
    w.u16(frame.value.kind().tag())?;
    w.u16(0)?;
    w.u32(length)?;
    w.u64(frame.record_no.get())?;
    w.u32(frame.segment_no.get())?;
    w.u32(0)?;
    w.bytes(&payload)?;
    let mut bytes = w.finish();
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}
