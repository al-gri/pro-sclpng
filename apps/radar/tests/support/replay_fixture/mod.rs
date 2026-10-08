//! Test/example-local construction with the accepted DTOs, writer and codec.
#![allow(dead_code)]

use std::error::Error;
use std::path::Path;

use domain::event::InputContext;
use domain::identity::*;
use domain::policy::WatermarkKind;
use domain::record::*;
use recording::{Crc32, PrefixSummary, WalWriter, encode_frame};

#[path = "../../../src/replay/profile.rs"]
pub mod profile;

pub const CLI_PROFILE: &str = profile::NAME;
pub const FIXTURE_MAX_BYTES: u64 = 64 * 1024;
pub const SNAPSHOT: &[u8] =
    include_bytes!("../../../../../tests/fixtures/bitget/books50-snapshot.json");
pub const UPDATE: &[u8] =
    include_bytes!("../../../../../tests/fixtures/bitget/books50-update.json");
pub const DUPLICATE: &[u8] =
    include_bytes!("../../../../../tests/fixtures/bitget/books50-duplicate.json");
pub const EMPTY: &[u8] =
    include_bytes!("../../../../../tests/fixtures/bitget/books50-empty-levels.json");
pub const ZERO: &[u8] =
    include_bytes!("../../../../../tests/fixtures/bitget/books50-zero-quantity-unknown.json");
pub const RESET: &[u8] = include_bytes!("../../../../../tests/fixtures/bitget/books50-reset.json");
pub const GAP: &[u8] = include_bytes!("../../../../../tests/fixtures/bitget/books50-gap.json");

pub fn record_no(value: u64) -> RecordNo {
    RecordNo::new(value).expect("small positive fixture record")
}

pub fn wire_context(sample_ns: u64) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(
            profile::SYNTHETIC_UNIX_BASE_NS
                .checked_add(i64::try_from(sample_ns).expect("fixture sample fits i64"))
                .expect("fixture Unix sample fits i64"),
        ),
        monotonic_ns: MonotonicNs::new(sample_ns),
        context: InputContext::Active(profile::active_context()),
    }
}

pub fn book_tag(epoch: u64) -> EpochTag {
    EpochTag {
        book: Some(BookEpoch::new(epoch).expect("positive fixture book epoch")),
        ..profile::stream_binding().tag
    }
}

pub fn frame(number: u64, value: Record) -> RecordFrame {
    RecordFrame {
        record_no: record_no(number),
        segment_no: SegmentNo::new(0),
        value,
    }
}

pub fn raw_frame(
    number: u64,
    attempt: u64,
    sample_ns: u64,
    tag: EpochTag,
    bytes: &[u8],
) -> RecordFrame {
    frame(
        number,
        Record::RawInput(RawInput {
            context: wire_context(sample_ns),
            stream: profile::stream_binding().id,
            tag,
            attempt: CaptureAttemptNo::new(attempt).expect("positive fixture attempt"),
            bytes: bytes.to_vec(),
        }),
    )
}

pub fn control_frame(number: u64, sample_ns: u64, value: Control) -> RecordFrame {
    frame(
        number,
        Record::Control(ControlRecord {
            context: wire_context(sample_ns),
            value,
        }),
    )
}

pub fn transport_frame(number: u64, sample_ns: u64, value: Transport) -> RecordFrame {
    let binding = profile::stream_binding();
    control_frame(
        number,
        sample_ns,
        Control::Transport {
            connection: binding.connection_id,
            epoch: binding.tag.connection,
            value,
        },
    )
}

pub fn book_epoch_frame(number: u64, sample_ns: u64, expected: u64, next: u64) -> RecordFrame {
    control_frame(
        number,
        sample_ns,
        Control::EpochAdvance {
            change: EpochChange::Book {
                owner: profile::stream_binding()
                    .book_id
                    .expect("fixture book stream"),
                expected: BookEpoch::new(expected).expect("positive old epoch"),
                next: BookEpoch::new(next).expect("positive new epoch"),
            },
            reason: Reason::UserReset,
        },
    )
}

/// Every stamp is synthetic and fixed; no host clock or environment is sampled.
pub fn fixture_prefix() -> Vec<RecordFrame> {
    vec![
        frame(
            1,
            Record::ArchiveStart(ArchiveStart {
                archive: profile::archive_id(),
                session: profile::session_id(),
                clock: profile::clock_id(),
                mode: profile::durability_mode(),
                previous_archive: None,
            }),
        ),
        frame(2, Record::InstrumentSpec(profile::instrument_spec())),
        frame(3, Record::StreamDefinition(profile::stream_definition())),
        frame(4, Record::ConfigDefinition(profile::config_definition())),
        transport_frame(5, 40, Transport::Up),
        raw_frame(6, 1, 100, book_tag(1), SNAPSHOT),
        raw_frame(7, 2, 110, book_tag(1), UPDATE),
        raw_frame(8, 3, 120, book_tag(1), DUPLICATE),
        raw_frame(9, 4, 130, book_tag(1), EMPTY),
        raw_frame(10, 5, 140, book_tag(1), ZERO),
        book_epoch_frame(11, 200, 1, 2),
        raw_frame(12, 6, 300, book_tag(2), SNAPSHOT),
        control_frame(
            13,
            400,
            Control::Timer {
                stream: profile::stream_binding().id,
                timer_id: 1,
                deadline_ns: 400,
            },
        ),
        transport_frame(14, 410, Transport::Down),
        transport_frame(15, 420, Transport::Up),
        book_epoch_frame(16, 430, 2, 3),
        raw_frame(17, 7, 440, book_tag(2), SNAPSHOT),
        raw_frame(18, 8, 450, book_tag(3), SNAPSHOT),
        raw_frame(19, 9, 460, book_tag(3), UPDATE),
        raw_frame(20, 10, 470, book_tag(3), RESET),
        book_epoch_frame(21, 480, 3, 4),
        raw_frame(22, 11, 490, book_tag(4), SNAPSHOT),
        raw_frame(23, 12, 500, book_tag(4), UPDATE),
        raw_frame(24, 13, 510, book_tag(4), GAP),
        frame(
            25,
            Record::Gap(Gap {
                context: wire_context(520),
                scope: GapScope::ExplicitTargets(vec![GapTarget {
                    stream: profile::stream_binding().id,
                    tag: book_tag(4),
                    range: Some((
                        CaptureAttemptNo::new(14).expect("first lost attempt"),
                        CaptureAttemptNo::new(15).expect("last lost attempt"),
                    )),
                    loss_count: Some(2),
                }]),
                reason: Reason::QueueOverflow,
            }),
        ),
        control_frame(
            26,
            530,
            Control::Recording(RecordingEvidence {
                health: RecordingHealth::Failed,
                kind: WatermarkKind::Written,
                through: Some(record_no(25)),
                reason: Reason::WriteFailure,
            }),
        ),
        control_frame(
            27,
            540,
            Control::Recording(RecordingEvidence {
                health: RecordingHealth::Healthy,
                kind: WatermarkKind::Written,
                through: Some(record_no(26)),
                reason: Reason::NoFault,
            }),
        ),
    ]
}

fn segment_seal(summary: PrefixSummary, number: u64) -> RecordFrame {
    frame(
        number,
        Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: summary.frame_count,
            prefix_physical_len: summary.physical_bytes,
            prefix_crc32: summary.crc32,
            prior_record: summary.prior_record.expect("nonempty fixture prefix"),
            has_gap: summary.has_gap,
            is_final: true,
        }),
    )
}

fn archive_seal(summary: PrefixSummary, number: u64) -> RecordFrame {
    frame(
        number,
        Record::ArchiveSeal(ArchiveSeal {
            expected_segment_count: 1,
            prior_frame_count: summary.frame_count,
            total_prefix_physical_bytes: summary.physical_bytes,
            prefix_crc32: summary.crc32,
            prior_record: summary.prior_record.expect("nonempty fixture prefix"),
            input_quality: if summary.has_gap {
                InputQuality::GapsRecorded
            } else {
                InputQuality::NoKnownLoss
            },
        }),
    )
}

/// Reuses the accepted frame codec for fixture seals, including protected-byte
/// CRC accounting. This is test scaffolding, never a recovery scanner.
fn prefix_summary(frames: &[RecordFrame]) -> PrefixSummary {
    let mut crc = Crc32::default();
    let mut summary = PrefixSummary::default();
    for frame in frames {
        let bytes = encode_frame(frame).expect("shape-valid fixture frame");
        crc.update(&bytes[..bytes.len().checked_sub(4).expect("frame checksum trailer")]);
        summary.frame_count = summary
            .frame_count
            .checked_add(1)
            .expect("small fixture count");
        summary.physical_bytes = summary
            .physical_bytes
            .checked_add(u64::try_from(bytes.len()).expect("frame size fits u64"))
            .expect("small fixture length");
        summary.prior_record = Some(frame.record_no);
        summary.has_gap |= matches!(frame.value, Record::Gap(_));
    }
    summary.crc32 = crc.digest();
    summary
}

/// Single-segment test helper: removes old seals, assigns dense record numbers,
/// and rebuilds both physical seals after a scenario mutation.
pub fn seal_frames(mut prefix: Vec<RecordFrame>) -> Vec<RecordFrame> {
    prefix.retain(|frame| !matches!(frame.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)));
    for (index, frame) in prefix.iter_mut().enumerate() {
        frame.record_no = record_no(u64::try_from(index).expect("small fixture index") + 1);
        frame.segment_no = SegmentNo::new(0);
    }
    let number = u64::try_from(prefix.len()).expect("small fixture count") + 1;
    prefix.push(segment_seal(prefix_summary(&prefix), number));
    prefix.push(archive_seal(prefix_summary(&prefix), number + 1));
    prefix
}

pub fn fixture_frames() -> Vec<RecordFrame> {
    seal_frames(fixture_prefix())
}

/// Builds the committed fixture exclusively through the accepted low-level
/// synthetic WAL writer. create_new rejects existing destinations.
pub fn write_fixture(path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
    let mut writer = WalWriter::create(path)?;
    for frame in fixture_prefix() {
        writer.append(&frame)?;
    }
    let summary = writer.prefix_summary();
    let seal_no = summary
        .frame_count
        .checked_add(1)
        .ok_or("fixture record overflow")?;
    writer.append(&segment_seal(summary, seal_no))?;
    writer.append(&archive_seal(
        writer.prefix_summary(),
        seal_no.checked_add(1).ok_or("fixture record overflow")?,
    ))?;
    if writer.prefix_summary().physical_bytes > FIXTURE_MAX_BYTES {
        return Err("synthetic fixture exceeds its 64 KiB cap".into());
    }
    writer.finish()?;
    Ok(())
}
