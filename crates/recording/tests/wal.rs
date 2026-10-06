use std::collections::BTreeSet;
use std::fs;
use std::panic;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use domain::artifact::ArtifactRef;
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, PolicyFields, RecordingGate, SilenceRule};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;
use recording::*;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TempFiles {
    paths: Vec<PathBuf>,
}

impl TempFiles {
    fn new() -> Self {
        Self { paths: Vec::new() }
    }

    fn path(&mut self, label: &str) -> PathBuf {
        let serial = NEXT_TEMP.fetch_add(1, AtomicOrdering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "proscalping-rec001b-{}-{label}-{serial}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        self.paths.push(path.clone());
        path
    }
}

impl Drop for TempFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

fn record(value: u64) -> RecordNo {
    RecordNo::new(value).expect("positive record")
}

fn segment(value: u32) -> SegmentNo {
    SegmentNo::new(value)
}

fn archive_id() -> ArchiveId {
    ArchiveId::new([1; 16]).expect("nonzero archive")
}

fn session_id() -> CaptureSessionId {
    CaptureSessionId::new([2; 16]).expect("nonzero session")
}

fn clock_id() -> ClockId {
    ClockId::new(1).expect("positive clock")
}

fn artifact() -> ArtifactRef {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("artifact grammar")
}

fn instrument() -> InstrumentRef {
    InstrumentRef {
        venue: Token::new("SYN").expect("token"),
        market: MarketKind::Spot,
        product_namespace: Token::new("spot").expect("token"),
        native_symbol: Token::new("ABCUSD").expect("token"),
    }
}

fn spec_ref(version: u32) -> SpecRef {
    SpecRef {
        instrument: instrument(),
        version: SpecVersion::new(version).expect("positive spec version"),
    }
}

fn numeric_spec(version: u32) -> NumericSpec {
    NumericSpec::new(NumericSpecFields {
        reference: spec_ref(version),
        price_units: PriceUnits {
            quote: Token::new("USD").expect("token"),
            basis: Token::new("BASE").expect("token"),
        },
        quantity_unit: Token::new("ABC").expect("token"),
        base_asset: Token::new("ABC").expect("token"),
        price_increment: ExactDecimal::ONE,
        quantity_increment: ExactDecimal::ONE,
        quantity_to_base_multiplier: Some(ExactDecimal::ONE),
    })
    .expect("valid numeric spec")
}

fn active_context() -> ActiveContext {
    ActiveContext {
        config: ConfigVersion::new(1).expect("positive config"),
        normalizer: NormalizerVersion::new(1).expect("positive normalizer"),
    }
}

fn bootstrap_context(unix_ns: i64, monotonic_ns: u64) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(unix_ns),
        monotonic_ns: MonotonicNs::new(monotonic_ns),
        context: InputContext::Bootstrap,
    }
}

fn active_wire_context(unix_ns: i64, monotonic_ns: u64) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(unix_ns),
        monotonic_ns: MonotonicNs::new(monotonic_ns),
        context: InputContext::Active(active_context()),
    }
}

fn start_frame(mode: DurabilityMode) -> RecordFrame {
    RecordFrame {
        record_no: record(1),
        segment_no: segment(0),
        value: Record::ArchiveStart(ArchiveStart {
            archive: archive_id(),
            session: session_id(),
            clock: clock_id(),
            mode,
            previous_archive: None,
        }),
    }
}

fn spec_frame(number: u64, version: u32, unix_ns: i64, monotonic_ns: u64) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(0),
        value: Record::InstrumentSpec(InstrumentSpecRecord {
            context: bootstrap_context(unix_ns, monotonic_ns),
            slot: InstrumentSlot::new(1).expect("slot"),
            numeric: numeric_spec(version),
            provenance: artifact(),
        }),
    }
}

fn stream_binding() -> StreamBinding {
    StreamBinding {
        id: StreamId::new(1).expect("stream"),
        instrument_slot: InstrumentSlot::new(1).expect("slot"),
        spec: spec_ref(1),
        connection_id: ConnectionId::new(1).expect("connection"),
        channel: Channel::BookNormal,
        book_id: Some(BookId::new(1).expect("book")),
        tag: EpochTag {
            spec: SpecVersion::new(1).expect("spec"),
            connection: ConnectionEpoch::new(1).expect("connection epoch"),
            subscription: SubscriptionEpoch::new(1).expect("subscription epoch"),
            book: Some(BookEpoch::new(1).expect("book epoch")),
        },
        feed_profile: FeedProfileVersion::new(1).expect("profile"),
    }
}

fn stream_frame(number: u64) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(0),
        value: Record::StreamDefinition(StreamDefinition {
            context: bootstrap_context(number as i64, number),
            binding: stream_binding(),
            provenance: artifact(),
        }),
    }
}

fn policy(gate: RecordingGate) -> PolicyFields {
    PolicyFields {
        silence_rule: SilenceRule::UnknownOnSilence,
        freshness_deadline_ns: Some(100),
        warmup_min_updates: Some(1),
        warmup_min_elapsed_ns: Some(0),
        allow_quiet_with_proof: false,
        require_two_sided_snapshot: true,
        recording_gate: gate,
    }
}

fn config_frame_at(number: u64, gate: RecordingGate) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(0),
        value: Record::ConfigDefinition(ConfigDefinition {
            context: bootstrap_context(number as i64, number),
            next: active_context(),
            provenance_kind: ProvenanceKind::Synthetic,
            evidence: artifact(),
            fields: policy(gate),
        }),
    }
}

fn config_frame(gate: RecordingGate) -> RecordFrame {
    config_frame_at(4, gate)
}

fn raw_frame(number: u64, attempt: u64, bytes: Vec<u8>) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(0),
        value: Record::RawInput(RawInput {
            context: active_wire_context(number as i64, number),
            stream: StreamId::new(1).expect("stream"),
            tag: stream_binding().tag,
            attempt: CaptureAttemptNo::new(attempt).expect("attempt"),
            bytes,
        }),
    }
}

fn raw_frame_in_segment(
    number: u64,
    segment_no: u32,
    attempt: u64,
    bytes: Vec<u8>,
) -> RecordFrame {
    let mut frame = raw_frame(number, attempt, bytes);
    frame.segment_no = segment(segment_no);
    frame
}

fn timer_frame(number: u64, segment_no: u32) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(segment_no),
        value: Record::Control(ControlRecord {
            context: active_wire_context(number as i64, number),
            value: Control::Timer {
                stream: StreamId::new(1).expect("stream"),
                timer_id: number,
                deadline_ns: number,
            },
        }),
    }
}

fn queue_gap_frame(number: u64, loss_count: Option<u64>) -> RecordFrame {
    RecordFrame {
        record_no: record(number),
        segment_no: segment(0),
        value: Record::Gap(Gap {
            context: active_wire_context(number as i64, number),
            scope: GapScope::ExplicitTargets(vec![GapTarget {
                stream: StreamId::new(1).expect("stream"),
                tag: stream_binding().tag,
                range: None,
                loss_count,
            }]),
            reason: Reason::QueueOverflow,
        }),
    }
}

fn prefix_stats(frames: &[RecordFrame]) -> (u64, u64, u32) {
    let mut count = 0_u64;
    let mut physical = 0_u64;
    let mut crc = Crc32::default();
    for frame in frames {
        let bytes = encode_frame(frame).expect("encode prefix");
        count = count.checked_add(1).expect("small fixture");
        physical = physical
            .checked_add(u64::try_from(bytes.len()).expect("frame length"))
            .expect("small fixture");
        crc.update(&bytes[..bytes.len() - 4]);
    }
    (count, physical, crc.digest())
}

fn seal_frame(
    number: u64,
    segment_no: u32,
    prefix: &[RecordFrame],
    has_gap: bool,
    is_final: bool,
) -> RecordFrame {
    let (count, physical, crc) = prefix_stats(prefix);
    let prior = prefix.last().expect("nonempty segment prefix").record_no;
    RecordFrame {
        record_no: record(number),
        segment_no: segment(segment_no),
        value: Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: count,
            prefix_physical_len: physical,
            prefix_crc32: crc,
            prior_record: prior,
            has_gap,
            is_final,
        }),
    }
}

fn archive_seal_frame(
    number: u64,
    segment_no: u32,
    prefix: &[RecordFrame],
    expected_segments: u32,
    quality: InputQuality,
) -> RecordFrame {
    let (count, physical, crc) = prefix_stats(prefix);
    let prior = prefix.last().expect("nonempty archive prefix").record_no;
    RecordFrame {
        record_no: record(number),
        segment_no: segment(segment_no),
        value: Record::ArchiveSeal(ArchiveSeal {
            expected_segment_count: expected_segments,
            prior_frame_count: count,
            total_prefix_physical_bytes: physical,
            prefix_crc32: crc,
            prior_record: prior,
            input_quality: quality,
        }),
    }
}

fn complete_frames() -> Vec<RecordFrame> {
    let mut frames = vec![
        start_frame(DurabilityMode::Buffered),
        spec_frame(2, 1, 2, 2),
        stream_frame(3),
        config_frame(RecordingGate::Written),
        raw_frame(5, 1, b"exchange_ts=200".to_vec()),
        timer_frame(6, 0),
        queue_gap_frame(7, None),
        raw_frame(8, 3, b"exchange_ts=100".to_vec()),
    ];
    let seal = seal_frame(9, 0, &frames, true, true);
    frames.push(seal);
    let archive_seal =
        archive_seal_frame(10, 0, &frames, 1, InputQuality::GapsRecorded);
    frames.push(archive_seal);
    frames
}

fn multi_segment_frames() -> (Vec<RecordFrame>, Vec<RecordFrame>) {
    let mut first = vec![
        start_frame(DurabilityMode::Buffered),
        spec_frame(2, 1, 2, 2),
        stream_frame(3),
        config_frame(RecordingGate::Written),
        raw_frame(5, 1, b"first".to_vec()),
    ];
    let first_seal = seal_frame(6, 0, &first, false, false);
    let first_seal_bytes = encode_frame(&first_seal).expect("encode seal");
    let first_seal_crc =
        scan_frame(&first_seal_bytes, 0).expect("scan seal").checksum;
    first.push(first_seal);

    let segment_start = RecordFrame {
        record_no: record(7),
        segment_no: segment(1),
        value: Record::SegmentStart(SegmentStart {
            archive: archive_id(),
            session: session_id(),
            clock: clock_id(),
            previous_segment: segment(0),
            previous_seal_record: record(6),
            previous_seal_crc32: first_seal_crc,
        }),
    };
    let mut second = vec![
        segment_start,
        raw_frame_in_segment(8, 1, 2, b"second".to_vec()),
    ];
    let second_seal = seal_frame(9, 1, &second, false, true);
    second.push(second_seal);

    let mut global = first.clone();
    global.extend(second.iter().cloned());
    let archive_seal =
        archive_seal_frame(10, 1, &global, 2, InputQuality::NoKnownLoss);
    second.push(archive_seal);
    (first, second)
}

fn encode_sequence(frames: &[RecordFrame]) -> Vec<u8> {
    let mut out = Vec::new();
    for frame in frames {
        out.extend_from_slice(&encode_frame(frame).expect("encode sequence"));
    }
    out
}

fn hex(text: &str) -> Vec<u8> {
    let digits: String = text.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    assert!(digits.len().is_multiple_of(2));
    digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("ascii hex");
            u8::from_str_radix(pair, 16).expect("hex")
        })
        .collect()
}

fn w01() -> Vec<u8> {
    hex(
        "
        50 53 52 57 01 00 01 00 01 00 00 00 26 00 00 00
        01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
        01 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01
        02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02
        01 00 00 00 03 00 13 c4 02 9e
        ",
    )
}

fn gap_positive_vector() -> Vec<u8> {
    hex(
        "
        50 53 52 57 01 00 01 00 07 00 00 00 40 00 00 00
        0b 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
        0b 00 00 00 00 00 00 00 0b 00 00 00 00 00 00 00
        01 00 00 00 01 00 00 00 01 04 01 00 01 00 00 00
        01 00 00 00 01 00 00 00 00 00 00 00 01 00 00 00
        00 00 00 00 01 01 00 00 00 00 00 00 00 00 00 00
        26 19 f4 e5
        ",
    )
}

fn gap_reversed_vector() -> Vec<u8> {
    hex(
        "
        50 53 52 57 01 00 01 00 07 00 00 00 40 00 00 00
        0b 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
        0b 00 00 00 00 00 00 00 0b 00 00 00 00 00 00 00
        01 00 00 00 01 00 00 00 04 01 01 00 01 00 00 00
        01 00 00 00 01 00 00 00 00 00 00 00 01 00 00 00
        00 00 00 00 01 01 00 00 00 00 00 00 00 00 00 00
        f2 eb ed 35
        ",
    )
}

fn rewrite_crc(bytes: &mut [u8]) {
    let protected_end = bytes.len() - 4;
    let checksum = crc32(&bytes[..protected_end]);
    bytes[protected_end..].copy_from_slice(&checksum.to_le_bytes());
}

fn write_bytes(path: &PathBuf, bytes: &[u8]) {
    fs::write(path, bytes).expect("write fixture");
}

fn standard_prefix_writer(path: &PathBuf) -> WalWriter {
    let mut writer = WalWriter::create(path).expect("create writer");
    for frame in [
        start_frame(DurabilityMode::Buffered),
        spec_frame(2, 1, 2, 2),
        stream_frame(3),
        config_frame(RecordingGate::Written),
    ] {
        writer.append(&frame).expect("append standard prefix");
    }
    writer
}

#[test]
fn w00_crc_and_w01_exact_round_trip() {
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    assert_eq!(crc32(b""), 0);

    let expected = w01();
    assert_eq!(expected.len(), 74);
    assert_eq!(crc32(&expected[..70]), 0x9e02_c413);
    let frame = start_frame(DurabilityMode::SyncBeforePublish);
    assert_eq!(encode_frame(&frame).expect("encode W01"), expected);
    assert_eq!(
        decode_exact(&expected, false, &Definitions::new()).expect("decode W01"),
        frame
    );

    let mut temp = TempFiles::new();
    let path = temp.path("w01-roundtrip");
    let mut writer = WalWriter::create(&path).expect("create");
    let marks = writer.append(&frame).expect("append");
    assert_eq!(marks.written, Some(record(1)));
    assert_eq!(marks.flushed, None);
    writer.flush().expect("flush");
    drop(writer);

    let read = read_all(&[&path]).expect("read");
    assert_eq!(read.records, vec![frame]);
    assert_eq!(read.report.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(read.report.physical_good_offset, 74);
}

#[test]
fn multiple_records_preserve_recorded_order_and_repeated_read_is_deterministic() {
    let frames = vec![
        start_frame(DurabilityMode::Buffered),
        spec_frame(2, 1, 200, 200),
        spec_frame(3, 2, 100, 100),
    ];
    let mut temp = TempFiles::new();
    let path = temp.path("order");
    let mut writer = WalWriter::create(&path).expect("create");
    for frame in &frames {
        writer.append(frame).expect("append");
    }
    writer.flush().expect("flush");
    drop(writer);

    let first = read_all(&[&path]).expect("first read");
    let second = read_all(&[&path]).expect("second read");
    assert_eq!(first, second);
    assert_eq!(first.records, frames);
    assert_eq!(
        first
            .records
            .iter()
            .map(|frame| frame.record_no.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn maximum_bounded_record_is_accepted_without_unbounded_length_trust() {
    let mut temp = TempFiles::new();
    let path = temp.path("max-record");
    let mut writer = standard_prefix_writer(&path);

    let empty = raw_frame(5, 1, Vec::new());
    let fixed_payload = encode_frame(&empty).expect("encode empty").len() - 36;
    let raw_len = MAX_PAYLOAD.checked_sub(fixed_payload).expect("fixed below cap");
    let maximum = raw_frame(5, 1, vec![0x5a; raw_len]);
    let encoded = encode_frame(&maximum).expect("encode max");
    assert_eq!(encoded.len(), MAX_FRAME_LEN);
    writer.append(&maximum).expect("append max");
    writer.flush().expect("flush");
    drop(writer);

    let read = read_all(&[&path]).expect("read max");
    assert_eq!(read.records.len(), 5);
    match &read.records[4].value {
        Record::RawInput(raw) => assert_eq!(raw.bytes.len(), raw_len),
        other => panic!("unexpected record: {other:?}"),
    }
}

#[test]
fn complete_archive_with_control_and_gap_round_trips_and_finishes_durable() {
    let frames = complete_frames();
    let mut temp = TempFiles::new();
    let path = temp.path("complete");
    let mut writer = WalWriter::create(&path).expect("create");
    for frame in &frames {
        writer.append(frame).expect("append complete archive");
    }
    let marks = writer.finish().expect("durable finish");
    assert_eq!(marks.durable, Some(record(10)));
    assert!(writer.is_closed());

    let read = read_all(&[&path]).expect("read complete");
    assert_eq!(read.records, frames);
    assert_eq!(read.report.status, ArchiveStatus::Complete);
    assert_eq!(read.report.input_quality, Some(InputQuality::GapsRecorded));
    assert_eq!(read.report.failure, None);
    assert_eq!(read.report.canonical_status, CanonicalStatus::NotEvaluated);
}

#[test]
fn checksum_corruption_is_explicit_and_not_a_clean_eof() {
    let mut bytes = w01();
    bytes[40] ^= 0x80;
    let mut temp = TempFiles::new();
    let path = temp.path("checksum");
    write_bytes(&path, &bytes);
    let read = read_all(&[&path]).expect("read");
    assert_eq!(read.report.status, ArchiveStatus::Corrupt);
    assert_eq!(read.report.physical_good_offset, 0);
    assert!(matches!(
        read.report.failure.as_ref().map(|failure| &failure.kind),
        Some(FailureKind::Codec(CodecError {
            kind: CodecErrorKind::ChecksumMismatch,
            ..
        }))
    ));
}

#[test]
fn w08_w01_every_cut_is_noarchive_or_truncated_tail() {
    let bytes = w01();
    let mut temp = TempFiles::new();
    let path = temp.path("w01-cuts");
    for cut in 0..bytes.len() {
        write_bytes(&path, &bytes[..cut]);
        let read = read_all(&[&path]).expect("read cut");
        if cut == 0 {
            assert_eq!(read.report.status, ArchiveStatus::NoArchive);
        } else {
            assert_eq!(
                read.report.status,
                ArchiveStatus::TruncatedTail,
                "cut={cut}"
            );
            assert_eq!(read.report.physical_good_offset, 0, "cut={cut}");
        }
    }
}

#[test]
fn w09_complete_archive_every_byte_cut_never_becomes_complete() {
    let frames = complete_frames();
    let bytes = encode_sequence(&frames);
    let mut boundaries = BTreeSet::new();
    let mut offset = 0_usize;
    for frame in &frames {
        offset += encode_frame(frame).expect("encode").len();
        boundaries.insert(offset);
    }
    assert_eq!(offset, bytes.len());

    let mut temp = TempFiles::new();
    let path = temp.path("archive-cuts");
    for cut in 0..bytes.len() {
        write_bytes(&path, &bytes[..cut]);
        let read = read_all(&[&path]).expect("read cut");
        assert_ne!(read.report.status, ArchiveStatus::Complete, "cut={cut}");
        if cut == 0 {
            assert_eq!(read.report.status, ArchiveStatus::NoArchive);
        } else if boundaries.contains(&cut) {
            assert!(
                matches!(
                    read.report.status,
                    ArchiveStatus::ValidPrefixIncomplete
                        | ArchiveStatus::SegmentSealedArchiveIncomplete
                ),
                "boundary cut={cut}, status={:?}",
                read.report.status
            );
        } else {
            assert_eq!(
                read.report.status,
                ArchiveStatus::TruncatedTail,
                "cut={cut}"
            );
        }
    }
}

#[test]
fn partial_header_body_and_checksum_keep_only_valid_prefix() {
    let first = encode_frame(&start_frame(DurabilityMode::Buffered)).expect("first");
    let second = encode_frame(&spec_frame(2, 1, 2, 2)).expect("second");
    let cuts = [5_usize, HEADER_LEN + 3, second.len() - 2];

    let mut temp = TempFiles::new();
    let path = temp.path("partial-final");
    for cut in cuts {
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second[..cut]);
        write_bytes(&path, &bytes);
        let read = read_all(&[&path]).expect("read");
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.report.status, ArchiveStatus::TruncatedTail);
        assert_eq!(
            read.report.physical_good_offset,
            u64::try_from(first.len()).expect("len")
        );
    }
}

#[test]
fn oversized_declared_lengths_and_absolute_offset_overflow_fail_before_allocation() {
    for length in [MAX_PAYLOAD as u32 + 1, u32::MAX] {
        let mut bytes = w01();
        bytes[12..16].copy_from_slice(&length.to_le_bytes());
        let error = scan_frame(&bytes, 0).expect_err("oversized length");
        assert_eq!(error.offset, 12);
        assert_eq!(error.kind, CodecErrorKind::LengthError);
    }

    let error = scan_frame(&w01(), u64::MAX - 10).expect_err("absolute overflow");
    assert_eq!(error.offset, 12);
    assert_eq!(error.kind, CodecErrorKind::LengthError);
}

#[test]
fn unsupported_versions_kinds_control_and_gap_scope_are_typed() {
    for (offset, field) in [(4_usize, "frame_version"), (6, "record_schema_version")] {
        let mut bytes = w01();
        bytes[offset..offset + 2].copy_from_slice(&2_u16.to_le_bytes());
        let error = scan_frame(&bytes, 0).expect_err("unsupported version");
        assert_eq!(
            error.kind,
            CodecErrorKind::Unsupported { field, value: 2 }
        );
    }

    let mut unknown_kind = w01();
    unknown_kind[8..10].copy_from_slice(&99_u16.to_le_bytes());
    let error = scan_frame(&unknown_kind, 0).expect_err("unsupported kind");
    assert_eq!(
        error.kind,
        CodecErrorKind::Unsupported {
            field: "record_kind",
            value: 99
        }
    );

    let mut control = encode_frame(&timer_frame(6, 0)).expect("control");
    control[56] = 255;
    rewrite_crc(&mut control);
    let error = decode_exact(&control, true, &Definitions::new())
        .expect_err("unsupported control");
    assert_eq!(
        error.kind,
        CodecErrorKind::Unsupported {
            field: "control_tag",
            value: 255
        }
    );

    let positive = gap_positive_vector();
    assert_eq!(crc32(&positive[..96]), 0xe5f4_1926);
    let decoded =
        decode_exact(&positive, true, &Definitions::new()).expect("accepted GAP vector");
    assert_eq!(decoded.record_no, record(11));
    assert!(matches!(decoded.value, Record::Gap(_)));

    let reversed = gap_reversed_vector();
    assert_eq!(crc32(&reversed[..96]), 0x35ed_ebf2);
    let error = decode_exact(&reversed, true, &Definitions::new())
        .expect_err("reversed GAP fields");
    assert_eq!(error.offset, 56);
    assert_eq!(
        error.kind,
        CodecErrorKind::Unsupported {
            field: "Gap.scope_kind",
            value: 4
        }
    );
}

#[test]
fn malformed_structural_fields_are_not_defaulted() {
    let mutations: &[(usize, u8, CodecErrorKind)] = &[
        (0, b'X', CodecErrorKind::Corrupt("magic")),
        (10, 1, CodecErrorKind::Corrupt("flags")),
        (28, 1, CodecErrorKind::Corrupt("reserved")),
    ];
    for (offset, value, expected) in mutations {
        let mut bytes = w01();
        bytes[*offset] = *value;
        rewrite_crc(&mut bytes);
        let error = scan_frame(&bytes, 0).expect_err("structural mutation");
        assert_eq!(&error.kind, expected);
    }

    let mut bad_option = w01();
    bad_option[69] = 2;
    rewrite_crc(&mut bad_option);
    let error = decode_exact(&bad_option, false, &Definitions::new())
        .expect_err("malformed option");
    assert_eq!(error.kind, CodecErrorKind::InvalidPayload("option"));
}

#[test]
fn valid_prefix_plus_corrupt_or_truncated_final_record_is_never_complete() {
    let first = encode_frame(&start_frame(DurabilityMode::Buffered)).expect("first");
    let second = encode_frame(&spec_frame(2, 1, 2, 2)).expect("second");
    let first_len = u64::try_from(first.len()).expect("len");
    let mut temp = TempFiles::new();
    let path = temp.path("prefix-final");

    let mut corrupt = first.clone();
    let mut bad_second = second.clone();
    bad_second[40] ^= 1;
    corrupt.extend_from_slice(&bad_second);
    write_bytes(&path, &corrupt);
    let read = read_all(&[&path]).expect("read corrupt");
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.report.status, ArchiveStatus::Corrupt);
    assert_eq!(read.report.physical_good_offset, first_len);

    let mut truncated = first;
    truncated.extend_from_slice(&second[..second.len() - 1]);
    write_bytes(&path, &truncated);
    let read = read_all(&[&path]).expect("read truncated");
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.report.status, ArchiveStatus::TruncatedTail);
    assert_eq!(read.report.physical_good_offset, first_len);
}

#[test]
fn reader_never_sorts_opaque_raw_inputs_by_timestamp_like_payloads() {
    let frames = vec![
        start_frame(DurabilityMode::Buffered),
        spec_frame(2, 1, 2, 2),
        stream_frame(3),
        config_frame(RecordingGate::Written),
        raw_frame(5, 1, b"exchange_ts=200".to_vec()),
        raw_frame(6, 2, b"exchange_ts=100".to_vec()),
    ];
    let mut temp = TempFiles::new();
    let path = temp.path("no-sort");
    let mut writer = WalWriter::create(&path).expect("create");
    for frame in &frames {
        writer.append(frame).expect("append");
    }
    writer.flush().expect("flush");
    drop(writer);

    let read = read_all(&[&path]).expect("read");
    let raws: Vec<Vec<u8>> = read
        .records
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::RawInput(value) => Some(value.bytes.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        raws,
        vec![b"exchange_ts=200".to_vec(), b"exchange_ts=100".to_vec()]
    );

}

#[test]
fn bounded_arbitrary_external_bytes_do_not_panic() {
    for len in 0_usize..=512 {
        let bytes: Vec<u8> = (0..len)
            .map(|index| ((index * 31 + len * 17) & 0xff) as u8)
            .collect();
        let result = panic::catch_unwind(|| {
            let _ = scan_frame(&bytes, 0);
        });
        assert!(result.is_ok(), "scan panic at len={len}");
    }

    let mut temp = TempFiles::new();
    let path = temp.path("arbitrary-reader");
    for len in 0_usize..=64 {
        let bytes: Vec<u8> = (0..len)
            .map(|index| ((index * 13 + len * 7) & 0xff) as u8)
            .collect();
        write_bytes(&path, &bytes);
        let result = panic::catch_unwind(|| {
            let _ = read_all(&[&path]);
        });
        assert!(result.is_ok(), "reader panic at len={len}");
    }
}

#[test]
fn middle_corruption_stops_before_valid_looking_later_magic() {
    let first = encode_frame(&start_frame(DurabilityMode::Buffered)).expect("first");
    let mut middle = encode_frame(&spec_frame(2, 1, 2, 2)).expect("middle");
    let last = encode_frame(&spec_frame(3, 2, 3, 3)).expect("last");
    middle[40] ^= 0x20;

    let mut bytes = first.clone();
    bytes.extend_from_slice(&middle);
    bytes.extend_from_slice(&last);

    let mut temp = TempFiles::new();
    let path = temp.path("no-rejoin");
    write_bytes(&path, &bytes);
    let read = read_all(&[&path]).expect("read");
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].record_no, record(1));
    assert_eq!(read.report.status, ArchiveStatus::Corrupt);
    assert_eq!(
        read.report.physical_good_offset,
        u64::try_from(first.len()).expect("len")
    );
}

#[test]
fn deleting_final_seals_never_promotes_a_valid_boundary_to_complete() {
    let frames = complete_frames();
    let mut temp = TempFiles::new();
    let path = temp.path("deleted-tail");

    write_bytes(&path, &encode_sequence(&frames[..frames.len() - 1]));
    let no_archive_seal = read_all(&[&path]).expect("read");
    assert_eq!(
        no_archive_seal.report.status,
        ArchiveStatus::SegmentSealedArchiveIncomplete
    );

    write_bytes(&path, &encode_sequence(&frames[..frames.len() - 2]));
    let no_seals = read_all(&[&path]).expect("read");
    assert_eq!(
        no_seals.report.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
}

#[test]
fn wrong_seal_values_and_trailing_data_are_explicitly_invalid() {
    let frames = complete_frames();
    let prefix = &frames[..8];
    let mut wrong = seal_frame(9, 0, prefix, true, true);
    match &mut wrong.value {
        Record::SegmentSeal(value) => value.prefix_frame_count += 1,
        _ => unreachable!(),
    }

    let mut bytes = encode_sequence(prefix);
    bytes.extend_from_slice(&encode_frame(&wrong).expect("own crc valid"));

    let mut temp = TempFiles::new();
    let path = temp.path("wrong-seal");
    write_bytes(&path, &bytes);
    let read = read_all(&[&path]).expect("read");
    assert_eq!(read.report.status, ArchiveStatus::Invalid);
    assert!(matches!(
        read.report.failure.as_ref().map(|failure| &failure.kind),
        Some(FailureKind::Validation(ValidationError::OrderOrChain(
            "segment_seal"
        )))
    ));

    let mut complete = encode_sequence(&frames);
    complete.push(0xff);
    write_bytes(&path, &complete);
    let read = read_all(&[&path]).expect("read trailing");
    assert_eq!(read.report.status, ArchiveStatus::Invalid);
    assert!(matches!(
        read.report.failure.as_ref().map(|failure| &failure.kind),
        Some(FailureKind::Validation(ValidationError::TrailingData))
    ));
}

#[test]
fn multisegment_chain_round_trips_and_wrong_segment_start_is_rejected() {
    let (first, second) = multi_segment_frames();
    let mut temp = TempFiles::new();
    let path0 = temp.path("segment-0");
    let path1 = temp.path("segment-1");

    let mut writer = WalWriter::create(&path0).expect("create segment0");
    for frame in &first {
        writer.append(frame).expect("append segment0");
    }
    writer.rotate(&path1).expect("rotate");
    for frame in &second {
        writer.append(frame).expect("append segment1");
    }
    writer.finish().expect("finish");

    let read = read_all(&[&path0, &path1]).expect("read multi");
    let mut expected = first.clone();
    expected.extend(second.iter().cloned());
    assert_eq!(read.records, expected);
    assert_eq!(read.report.status, ArchiveStatus::Complete);
    assert_eq!(read.report.input_quality, Some(InputQuality::NoKnownLoss));

    let bad0 = temp.path("bad-segment-0");
    let bad1 = temp.path("bad-segment-1");
    write_bytes(&bad0, &encode_sequence(&first));
    let mut bad_start = second[0].clone();
    match &mut bad_start.value {
        Record::SegmentStart(value) => value.previous_seal_crc32 ^= 1,
        _ => unreachable!(),
    }
    write_bytes(&bad1, &encode_frame(&bad_start).expect("encode bad start"));
    let bad = read_all(&[&bad0, &bad1]).expect("read bad chain");
    assert_eq!(bad.report.status, ArchiveStatus::Invalid);
    assert!(matches!(
        bad.report.failure.as_ref().map(|failure| &failure.kind),
        Some(FailureKind::Validation(ValidationError::OrderOrChain(
            "segment_start"
        )))
    ));
}

#[test]
fn local_gap_accounting_is_one_use_and_scope_changes_cannot_cross_open_window() {
    let mut temp = TempFiles::new();
    let path = temp.path("loss-accounting");
    let mut writer = standard_prefix_writer(&path);
    writer
        .append(&raw_frame(5, 1, b"one".to_vec()))
        .expect("raw1");
    writer
        .append(&queue_gap_frame(6, None))
        .expect("open local window");
    writer
        .append(&raw_frame(7, 3, b"three".to_vec()))
        .expect("window closes once");
    let error = writer
        .append(&raw_frame(8, 100, b"jump".to_vec()))
        .expect_err("permit cannot be reused");
    assert!(matches!(
        error,
        WriterError::Validation(ValidationError::Loss(
            LossError::UnaccountedAttemptGap
        ))
    ));

    let path = temp.path("loss-count");
    let mut writer = standard_prefix_writer(&path);
    writer
        .append(&raw_frame(5, 1, b"one".to_vec()))
        .expect("raw1");
    writer
        .append(&queue_gap_frame(6, Some(2)))
        .expect("window with claimed count");
    let error = writer
        .append(&raw_frame(7, 3, b"three".to_vec()))
        .expect_err("claimed two but inferred one");
    assert!(matches!(
        error,
        WriterError::Validation(ValidationError::Loss(
            LossError::LossCountMismatch
        ))
    ));

    let path = temp.path("gap-scope");
    let mut writer = standard_prefix_writer(&path);
    writer
        .append(&raw_frame(5, 1, b"one".to_vec()))
        .expect("raw1");
    writer
        .append(&queue_gap_frame(6, None))
        .expect("open window");
    let advance = RecordFrame {
        record_no: record(7),
        segment_no: segment(0),
        value: Record::Control(ControlRecord {
            context: active_wire_context(7, 7),
            value: Control::EpochAdvance {
                change: EpochChange::Connection {
                    owner: ConnectionId::new(1).expect("connection"),
                    expected: ConnectionEpoch::new(1).expect("epoch"),
                    next: ConnectionEpoch::new(2).expect("epoch"),
                },
                reason: Reason::Reconnect,
            },
        }),
    };
    let error = writer
        .append(&advance)
        .expect_err("open window cannot cross epoch");
    assert!(matches!(
        error,
        WriterError::Validation(ValidationError::Loss(
            LossError::GapScopeTransition
        ))
    ));
}

#[test]
fn archive_mode_rejects_weaker_recording_gates_without_changing_wire_tags() {
    let cases = [
        (DurabilityMode::Buffered, RecordingGate::Written, true),
        (DurabilityMode::Buffered, RecordingGate::Flushed, true),
        (DurabilityMode::Buffered, RecordingGate::Durable, true),
        (DurabilityMode::GroupSynced, RecordingGate::Written, false),
        (DurabilityMode::GroupSynced, RecordingGate::Flushed, false),
        (DurabilityMode::GroupSynced, RecordingGate::Durable, true),
        (
            DurabilityMode::SyncBeforePublish,
            RecordingGate::Written,
            false,
        ),
        (
            DurabilityMode::SyncBeforePublish,
            RecordingGate::Flushed,
            false,
        ),
        (
            DurabilityMode::SyncBeforePublish,
            RecordingGate::Durable,
            true,
        ),
    ];

    let mut temp = TempFiles::new();
    for (index, (mode, gate, valid)) in cases.into_iter().enumerate() {
        let path = temp.path(&format!("mode-gate-{index}"));
        let mut writer = WalWriter::create(&path).expect("create");
        writer.append(&start_frame(mode)).expect("start");
        let result = writer.append(&config_frame_at(2, gate));
        assert_eq!(result.is_ok(), valid, "mode={mode:?}, gate={gate:?}");
    }
}

#[test]
fn aggregate_crc_excludes_individual_frame_trailers() {
    let frames = complete_frames();
    let prefix = &frames[..8];
    let mut wrong_crc = Crc32::default();
    for frame in prefix {
        wrong_crc.update(&encode_frame(frame).expect("encode"));
    }
    let mut seal = seal_frame(9, 0, prefix, true, true);
    match &mut seal.value {
        Record::SegmentSeal(value) => {
            assert_ne!(value.prefix_crc32, wrong_crc.digest());
            value.prefix_crc32 = wrong_crc.digest();
        }
        _ => unreachable!(),
    }

    let mut bytes = encode_sequence(prefix);
    bytes.extend_from_slice(&encode_frame(&seal).expect("encode seal"));
    let mut temp = TempFiles::new();
    let path = temp.path("trailer-crc");
    write_bytes(&path, &bytes);
    let read = read_all(&[&path]).expect("read");
    assert_eq!(read.report.status, ArchiveStatus::Invalid);
    assert!(matches!(
        read.report.failure.as_ref().map(|failure| &failure.kind),
        Some(FailureKind::Validation(ValidationError::OrderOrChain(
            "segment_seal"
        )))
    ));
}

#[test]
fn writer_watermarks_are_truthful_and_existing_file_is_never_reopened_for_append() {
    let mut temp = TempFiles::new();
    let path = temp.path("watermarks");
    let frame = start_frame(DurabilityMode::Buffered);
    let mut writer = WalWriter::create(&path).expect("create");
    let marks = writer.append(&frame).expect("append");
    assert_eq!(marks.accepted, Some(record(1)));
    assert_eq!(marks.appended, Some(record(1)));
    assert_eq!(marks.written, Some(record(1)));
    assert_eq!(marks.flushed, None);
    assert_eq!(marks.durable, None);

    let marks = writer.flush().expect("flush");
    assert_eq!(marks.flushed, Some(record(1)));
    assert_eq!(marks.durable, None);

    let marks = writer.sync_all().expect("sync");
    assert_eq!(marks.durable, Some(record(1)));
    drop(writer);

    let error = match WalWriter::create(&path) {
        Ok(_) => panic!("existing WAL must not be reopened for append"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
}
