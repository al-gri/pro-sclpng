use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use domain::identity::{
    CaptureAttemptNo, Channel, ConfigVersion, FeedProfileVersion, RecordNo, SpecVersion, StreamId,
    Token,
};
use domain::policy::WatermarkKind;
use domain::qualified::NumericSpec;
use domain::record::*;
use recording::{ArchiveStatus, FailureKind, PhysicalReport, WalReader, crc32, encode_frame};

#[path = "support/replay_fixture/mod.rs"]
mod replay_fixture;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
const PROFILE: &str = "synthetic-rec001f1-v1";

struct TempWal(PathBuf);

impl TempWal {
    fn new(label: &str) -> Self {
        let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "proscalping-rec001f1-{}-{serial}-{label}.wal",
            std::process::id()
        )))
    }

    fn bytes(label: &str, bytes: &[u8]) -> Self {
        let file = Self::new(label);
        fs::write(&file.0, bytes).expect("write isolated test WAL");
        file
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/replay")
        .join(name)
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_radar-replay"))
        .args(args)
        .output()
        .expect("start real radar-replay process")
}

fn replay(path: &Path) -> Output {
    run(&[
        "--wal",
        path.to_str().expect("UTF-8 fixture path"),
        "--profile",
        PROFILE,
    ])
}

fn stdout(output: &Output) -> &str {
    std::str::from_utf8(&output.stdout).expect("UTF-8 deterministic report")
}

fn record_section(report: &str, number: u64) -> &str {
    let marker = format!("record={number} ");
    let start = report.find(&marker).expect("record trace exists");
    let remaining = &report[start..];
    let length = remaining.find("\nrecord=").unwrap_or(remaining.len());
    &remaining[..length]
}

fn assert_line(report: &str, expected: impl AsRef<str>) {
    let expected = expected.as_ref();
    assert!(
        report.lines().any(|line| line == expected),
        "missing {expected:?}\n{report}"
    );
}

fn assert_unverified(report: &str) {
    assert_line(report, "canonical_status=NotEvaluated");
    assert_line(report, "canonical_applicability=BLOCKED_UNVERIFIED");
    assert_line(report, "usable_data=false");
    for forbidden in [
        "usable_data=true",
        "SnapshotReleased",
        "UpdateReleased",
        "BookBecameUsable",
        "witness: Some(",
        "anchor: Some(",
        "publication_permit=true",
    ] {
        assert!(
            !report.contains(forbidden),
            "forbidden readiness {forbidden}:\n{report}"
        );
    }
    assert!(!report.contains("panicked"), "{report}");
}

fn encode(records: &[RecordFrame]) -> Vec<u8> {
    records
        .iter()
        .flat_map(|frame| encode_frame(frame).expect("encode accepted DTO shape"))
        .collect()
}

fn offsets(records: &[RecordFrame]) -> Vec<usize> {
    let mut total = 0;
    records
        .iter()
        .map(|frame| {
            let start = total;
            total += encode_frame(frame).expect("fixture frame").len();
            start
        })
        .collect()
}

fn physical(path: &Path) -> (PhysicalReport, Vec<RecordFrame>) {
    let mut reader = WalReader::open(path).expect("open test WAL using accepted reader");
    let mut accepted = Vec::new();
    while let Ok(Some(frame)) = reader.next_record() {
        accepted.push(frame);
    }
    (reader.report().clone(), accepted)
}

fn assert_physical(report: &str, expected: &PhysicalReport) {
    assert_line(report, format!("physical_status={:?}", expected.status));
    assert_line(
        report,
        format!("input_quality={:?}", expected.input_quality),
    );
    assert_line(
        report,
        format!("last_good_offset={}", expected.physical_good_offset),
    );
    assert_line(
        report,
        format!("framing_good_offset={}", expected.framing_good_offset),
    );
    assert_line(report, format!("last_record={:?}", expected.last_record));
    assert_line(
        report,
        format!("physical_frontier={:?}", expected.last_record),
    );
    assert_line(report, format!("failure={:?}", expected.failure));
}

fn check_physical_failure(label: &str, bytes: &[u8], status: ArchiveStatus) -> PhysicalReport {
    let file = TempWal::bytes(label, bytes);
    let (expected, accepted) = physical(&file.0);
    assert_eq!(expected.status, status, "{label}: {expected:?}");
    let output = replay(&file.0);
    assert_ne!(output.status.code(), Some(0), "{label}: {output:?}");
    assert_ne!(
        output.status.code(),
        Some(2),
        "WAL failure must not be CLI failure: {output:?}"
    );
    let report = stdout(&output);
    assert_physical(report, &expected);
    assert_unverified(report);
    assert_line(report, "diagnostic_prefix_complete=false");
    assert_line(
        report,
        format!("semantic_frontier={:?}", expected.last_record),
    );
    assert_eq!(
        report
            .lines()
            .filter(|line| line.starts_with("health_snapshot="))
            .count(),
        accepted.len(),
        "no rejected record or suffix may reach reducer\n{report}"
    );
    expected
}

fn prefix_through_first_raw() -> Vec<RecordFrame> {
    let mut records = replay_fixture::fixture_prefix();
    let first = records
        .iter()
        .position(|frame| matches!(frame.value, Record::RawInput(_)))
        .expect("raw fixture");
    records.truncate(first + 1);
    records
}

fn first_raw(records: &[RecordFrame]) -> &RawInput {
    records
        .iter()
        .find_map(|frame| match &frame.value {
            Record::RawInput(raw) => Some(raw),
            _ => None,
        })
        .expect("raw fixture")
}

fn next_control(records: &[RecordFrame], value: Control) -> RecordFrame {
    let last = records.last().expect("nonempty prefix");
    RecordFrame {
        record_no: last
            .record_no
            .checked_next()
            .expect("small test record number"),
        segment_no: last.segment_no,
        value: Record::Control(ControlRecord {
            context: first_raw(records).context,
            value,
        }),
    }
}

fn blocked_case(label: &str, prefix: Vec<RecordFrame>, block: RecordNo, reason: &str) -> String {
    let records = replay_fixture::seal_frames(prefix);
    let file = TempWal::bytes(label, &encode(&records));
    let (expected, _) = physical(&file.0);
    assert_eq!(
        expected.status,
        ArchiveStatus::Complete,
        "negative must reach semantic guard: {expected:?}"
    );
    let output = replay(&file.0);
    assert_ne!(output.status.code(), Some(0), "{label}: {output:?}");
    assert_ne!(output.status.code(), Some(2), "{output:?}");
    let report = stdout(&output);
    assert_physical(report, &expected);
    assert_line(report, format!("semantic_block_record={:?}", Some(block)));
    assert_line(report, format!("semantic_block_reason=Some(\"{reason}\")"));
    assert_line(
        report,
        format!(
            "semantic_frontier={:?}",
            RecordNo::new(block.get() - 1).ok()
        ),
    );
    assert_line(report, "diagnostic_prefix_complete=false");
    assert_unverified(report);
    let steps = report
        .lines()
        .filter(|line| line.starts_with("health_snapshot="))
        .count();
    assert_eq!(
        steps,
        usize::try_from(block.get() - 1).expect("small prefix")
    );
    assert!(report.contains(&format!(
        "record={} kind=",
        records.last().expect("seal").record_no.get()
    )));
    assert!(
        report.contains("semantic=NotApplied"),
        "physical suffix must be labelled unapplied\n{report}"
    );
    report.to_owned()
}

#[test]
fn two_fresh_processes_match_committed_golden_and_writer_rebuild() {
    let original = fixture_path("synthetic-v1.wal");
    let bytes = fs::read(&original).expect("committed synthetic WAL");
    assert!(bytes.len() <= 65_536, "saved fixture cap");
    let rebuilt = TempWal::new("rebuild");
    replay_fixture::write_fixture(&rebuilt.0).expect("accepted writer builds fixture");
    assert_eq!(fs::read(&rebuilt.0).expect("rebuilt bytes"), bytes);
    let one = replay(&original);
    let two = replay(&rebuilt.0);
    assert_eq!(one.status.code(), Some(0), "{one:?}");
    assert_eq!(two.status.code(), Some(0), "{two:?}");
    assert!(one.stderr.is_empty(), "{one:?}");
    assert!(two.stderr.is_empty(), "{two:?}");
    assert_eq!(
        one.stdout, two.stdout,
        "independent processes and different paths"
    );
    assert_eq!(
        one.stdout,
        fs::read(fixture_path("synthetic-v1.expected.txt")).expect("golden")
    );
    let report = stdout(&one);
    assert_unverified(report);
    assert_line(report, "diagnostic_format=synthetic-rec001f1-v1");
    assert_line(report, "diagnostic_prefix_complete=true");
    assert!(
        !report.contains(original.to_str().expect("path")),
        "absolute path leaked"
    );
    assert!(
        !report.contains(rebuilt.0.to_str().expect("path")),
        "absolute path leaked"
    );
}

#[test]
fn fixture_preserves_record_order_samples_lexemes_and_negative_health() {
    let records = replay_fixture::fixture_frames();
    let output = replay(&fixture_path("synthetic-v1.wal"));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = stdout(&output);
    let mut previous_sample = 0;
    for frame in &records {
        let sample = frame
            .value
            .context()
            .map_or(previous_sample, |context| context.monotonic_ns.get());
        previous_sample = sample;
        assert_line(
            report,
            format!(
                "record={} kind={:?} sample_ns={sample}",
                frame.record_no.get(),
                frame.value.kind()
            ),
        );
        if let Record::RawInput(raw) = &frame.value {
            let decoded =
                market_data::decode_message(&raw.bytes).expect("accepted lexical fixture");
            let market_data::BitgetMessage::Books50(frame) = decoded else {
                panic!("book fixture");
            };
            assert_line(report, format!("lexical={frame:?}"));
        }
    }
    let record_lines: Vec<_> = report
        .lines()
        .filter(|line| line.starts_with("record="))
        .collect();
    assert_eq!(record_lines.len(), records.len());
    for (frame, line) in records.iter().zip(record_lines) {
        assert!(
            line.starts_with(&format!("record={} ", frame.record_no.get())),
            "recorded order: {line}"
        );
    }
    assert_eq!(
        report
            .lines()
            .filter(|line| line.starts_with("health_snapshot="))
            .count(),
        records.len()
    );
    for diagnostic in [
        "DuplicateObservation",
        "PendingOverflow(Frames)",
        "PendingTimeout",
        "TransportDown",
        "StreamEpochReset",
        "ObsoleteScope",
        "ResetOrDiscontinuity",
        "ContinuityGap",
        "Gap(QueueOverflow)",
    ] {
        assert!(
            report.contains(diagnostic),
            "missing {diagnostic}\n{report}"
        );
    }
    assert!(report.contains("LexicalValue(\"1.2500\")"));
    assert!(report.contains("LexicalValue(\"0\")"));
    assert!(
        report.contains("Timer { stream: StreamId(1), timer_id: 1, deadline_ns: 400 }"),
        "original timer fields\n{report}"
    );
    assert_unverified(report);
}

#[test]
fn recording_failed_latch_survives_later_healthy_observation() {
    let output = replay(&fixture_path("synthetic-v1.wal"));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let lines: Vec<_> = stdout(&output)
        .lines()
        .filter(|line| line.starts_with("recording_observed="))
        .collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("health: Failed"));
    assert!(lines[0].contains("kind: Written, through: Some(RecordNo(25))"));
    assert!(lines[1].contains("health: Healthy"));
    assert!(lines[1].contains("kind: Written, through: Some(RecordNo(26))"));
    for line in lines {
        assert!(
            line.ends_with("recording_effective=Failed failed_latched=true"),
            "{line}"
        );
    }
    assert_unverified(stdout(&output));
}

#[test]
fn fixture_barriers_and_pending_cardinality_have_exact_record_transitions() {
    let output = replay(&fixture_path("synthetic-v1.wal"));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = stdout(&output);
    let empty_pending = "pending: PendingSummary { frames: 0, raw_bytes: 0, outputs: 0 }";
    for (number, count) in [(6, 1), (7, 2), (8, 2), (9, 3)] {
        let section = record_section(report, number);
        assert!(
            section.contains(&format!("pending: PendingSummary {{ frames: {count},")),
            "{section}"
        );
        assert!(
            section.contains(&format!("outputs: {count} }}")),
            "{section}"
        );
        assert!(
            section.contains("anchor: None, progress: 0, witness: None"),
            "{section}"
        );
    }
    let overflow = record_section(report, 10);
    assert!(
        overflow.contains("Invalid(PendingOverflow(Frames))"),
        "{overflow}"
    );
    assert!(overflow.contains("barrier: RecordNo(10)"));
    assert!(overflow.contains(empty_pending));
    let timeout = record_section(report, 13);
    assert!(timeout.contains("evaluation_ns: 400"));
    assert!(timeout.contains("Invalid(PendingTimeout)"));
    assert!(timeout.contains("barrier: RecordNo(13)"));
    assert!(timeout.contains(empty_pending));
    let down = record_section(report, 14);
    assert!(down.contains("transport: Down"));
    assert!(down.contains("Invalid(TransportDown)"));
    assert!(down.contains("barrier: RecordNo(14)"));
    let up = record_section(report, 15);
    assert!(up.contains("transport: Up"));
    assert!(up.contains("Invalid(TransportDown)"));
    assert!(up.contains("barrier: RecordNo(14)"));
    let epoch = record_section(report, 16);
    assert!(epoch.contains("book: Some(BookEpoch(3))"));
    assert!(epoch.contains("book: Some(NoSnapshot)"));
    assert!(epoch.contains("barrier: RecordNo(16)"));
    let old = record_section(report, 17);
    assert!(old.contains("ObsoleteScope { stream: StreamId(1) }"));
    assert!(old.contains("barrier: RecordNo(16)"));
    assert!(old.contains(empty_pending));
    for (number, reason) in [
        (20, "ResetOrDiscontinuity"),
        (24, "ContinuityGap"),
        (25, "Gap(QueueOverflow)"),
    ] {
        let section = record_section(report, number);
        assert!(section.contains(&format!("Invalid({reason})")), "{section}");
        assert!(
            section.contains(&format!("barrier: RecordNo({number})")),
            "{section}"
        );
        assert!(section.contains(empty_pending), "{section}");
    }
    assert_unverified(report);
}

#[test]
fn recorded_timer_expiry_is_exclusive_and_does_not_infer_transport_down() {
    let initial = prefix_through_first_raw();
    let raw = first_raw(&initial);
    let threshold = raw.context.monotonic_ns.get() + replay_fixture::profile::PENDING_WAIT_NS;
    for sample in [threshold - 1, threshold, threshold + 1] {
        let mut records = initial.clone();
        let number = records.last().expect("raw").record_no.get() + 1;
        records.push(replay_fixture::control_frame(
            number,
            sample,
            Control::Timer {
                stream: raw.stream,
                timer_id: 1,
                deadline_ns: sample,
            },
        ));
        let file = TempWal::bytes(
            "timer-boundary",
            &encode(&replay_fixture::seal_frames(records)),
        );
        let output = replay(&file.0);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let report = stdout(&output);
        let section = record_section(report, number);
        assert!(
            section.contains(&format!("evaluation_ns: {sample}")),
            "{section}"
        );
        assert!(
            section.contains("transport: Up"),
            "health timer is not physical timeout/down\n{section}"
        );
        if sample < threshold {
            assert!(section.contains("pending: PendingSummary { frames: 1,"));
            assert!(!section.contains("PendingTimeout"));
        } else {
            assert!(section.contains("Invalid(PendingTimeout)"));
            assert!(section.contains(&format!("barrier: RecordNo({number})")));
            assert!(
                section.contains("pending: PendingSummary { frames: 0, raw_bytes: 0, outputs: 0 }")
            );
        }
        assert_unverified(report);
    }
}

#[test]
fn help_succeeds_and_malformed_cli_is_exit_two() {
    let help = run(&["--help"]);
    assert_eq!(help.status.code(), Some(0), "{help:?}");
    assert!(help.stderr.is_empty());
    assert!(stdout(&help).contains("radar-replay"));
    assert!(stdout(&help).contains("--wal"));
    assert!(stdout(&help).contains("--profile"));
    let cases: &[&[&str]] = &[
        &[],
        &["--wal"],
        &["--profile"],
        &["--wal", "fixture"],
        &["--profile", PROFILE],
        &["--wal", "fixture", "--profile", "unknown"],
        &["--wal", "fixture", "--profile", PROFILE, "--wal", "fixture"],
        &[
            "--wal",
            "fixture",
            "--profile",
            PROFILE,
            "--profile",
            PROFILE,
        ],
        &["--wal", "fixture", "--profile", PROFILE, "--live"],
        &["--wal", "fixture", "--profile", PROFILE, "--mode", "live"],
        &[
            "--wal",
            "fixture",
            "--profile",
            PROFILE,
            "--socket",
            "wss://example.invalid",
        ],
        &["--wal=fixture", "--profile", PROFILE],
        &["--wal", "", "--profile", PROFILE],
        &["--wal", "fixture", "--profile", PROFILE, "trailing"],
        &["--", "--wal", "fixture", "--profile", PROFILE],
        &["--help", "--live"],
        &["--help", "--help"],
    ];
    for args in cases {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        assert!(!output.stderr.is_empty(), "{args:?}: {output:?}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}

#[test]
fn missing_path_is_a_nonzero_diagnostic_without_panic() {
    let missing = TempWal::new("missing");
    let output = replay(&missing.0);
    assert_ne!(output.status.code(), Some(0), "{output:?}");
    assert_ne!(output.status.code(), Some(2), "{output:?}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
}

#[test]
fn torn_header_payload_and_trailer_stop_at_prior_frame_boundary() {
    let records = replay_fixture::fixture_frames();
    let locations = offsets(&records);
    let bytes = encode(&records);
    let target = records
        .iter()
        .position(|frame| matches!(frame.value, Record::RawInput(_)))
        .expect("raw");
    let start = locations[target];
    let length = encode_frame(&records[target]).expect("frame").len();
    for (label, cut) in [
        ("header", start + 17),
        ("payload", start + 33),
        ("trailer", start + length - 1),
    ] {
        let result = check_physical_failure(label, &bytes[..cut], ArchiveStatus::TruncatedTail);
        assert_eq!(
            result.physical_good_offset,
            u64::try_from(start).expect("offset")
        );
        assert_eq!(result.last_record, Some(records[target - 1].record_no));
        assert!(
            matches!(result.failure.expect("torn failure").kind, FailureKind::Codec(error) if error.kind == recording::CodecErrorKind::TruncatedTail)
        );
    }
}

#[test]
fn empty_and_whole_missing_final_records_remain_incomplete() {
    check_physical_failure("empty", &[], ArchiveStatus::NoArchive);
    let records = replay_fixture::fixture_frames();
    for count in [records.len() - 1, records.len() - 2] {
        let expected_status = if count == records.len() - 1 {
            ArchiveStatus::SegmentSealedArchiveIncomplete
        } else {
            ArchiveStatus::ValidPrefixIncomplete
        };
        let result = check_physical_failure(
            "whole-final-removal",
            &encode(&records[..count]),
            expected_status,
        );
        assert_eq!(
            result.input_quality, None,
            "unsealed quality stays optional"
        );
        assert!(result.failure.is_none());
    }
}

#[test]
fn trailing_bytes_require_actual_eof_validation_after_archive_seal() {
    let records = replay_fixture::fixture_frames();
    let mut bytes = encode(&records);
    let sealed_length = bytes.len();
    bytes.extend_from_slice(b"PSRW");
    let result = check_physical_failure("trailing", &bytes, ArchiveStatus::Invalid);
    assert_eq!(
        result.physical_good_offset,
        u64::try_from(sealed_length).expect("length")
    );
    assert_eq!(
        result.last_record,
        Some(records.last().expect("seal").record_no)
    );
    assert!(matches!(
        result.failure.expect("trailing error").kind,
        FailureKind::Validation(recording::ValidationError::TrailingData)
    ));
}

#[test]
fn middle_checksum_corruption_never_rejoins_valid_suffix() {
    let records = replay_fixture::fixture_frames();
    let locations = offsets(&records);
    let target = records
        .iter()
        .position(|frame| matches!(frame.value, Record::RawInput(_)))
        .expect("raw");
    let mut bytes = encode(&records);
    bytes[locations[target] + 32] ^= 1;
    let result = check_physical_failure("middle-crc", &bytes, ArchiveStatus::Corrupt);
    assert_eq!(result.last_record, Some(records[target - 1].record_no));
    assert!(
        matches!(result.failure.expect("checksum error").kind, FailureKind::Codec(error) if error.kind == recording::CodecErrorKind::ChecksumMismatch)
    );
}

#[test]
fn unsupported_frame_schema_kind_and_control_keep_typed_reader_failure() {
    let records = replay_fixture::fixture_frames();
    let locations = offsets(&records);
    let target = records
        .iter()
        .position(|frame| matches!(frame.value, Record::Control(_)))
        .expect("control");
    for (label, offset, replacement) in [
        ("version", 4, 2),
        ("schema", 6, 2),
        ("kind", 8, 255),
        ("control", 56, 255),
    ] {
        let mut bytes = encode(&records);
        bytes[locations[target] + offset] = replacement;
        if offset == 56 {
            let end = locations[target + 1];
            let checksum = crc32(&bytes[locations[target]..end - 4]);
            bytes[end - 4..end].copy_from_slice(&checksum.to_le_bytes());
        }
        let result = check_physical_failure(label, &bytes, ArchiveStatus::Unsupported);
        assert_eq!(result.last_record, Some(records[target - 1].record_no));
        assert!(
            matches!(result.failure.expect("unsupported error").kind, FailureKind::Codec(error) if matches!(error.kind, recording::CodecErrorKind::Unsupported { .. }))
        );
    }
}

#[test]
fn record_order_attempt_accounting_and_seal_claim_failures_are_not_repaired() {
    let original = replay_fixture::fixture_frames();
    let first = original
        .iter()
        .position(|frame| matches!(frame.value, Record::RawInput(_)))
        .expect("raw");
    let mut wrong_order = original.clone();
    wrong_order[first].record_no = wrong_order[first - 1].record_no;
    let result = check_physical_failure("order", &encode(&wrong_order), ArchiveStatus::Invalid);
    assert!(format!("{:?}", result.failure).contains("record_no"));
    let mut attempts = original.clone();
    let Record::RawInput(raw) = &mut attempts[first].value else {
        unreachable!();
    };
    raw.attempt = CaptureAttemptNo::new(2).expect("attempt");
    let result = check_physical_failure("attempt-hole", &encode(&attempts), ArchiveStatus::Invalid);
    assert!(format!("{:?}", result.failure).contains("UnaccountedAttemptGap"));
    let mut seals = original;
    let target = seals
        .iter_mut()
        .find(|frame| matches!(frame.value, Record::SegmentSeal(_)))
        .expect("seal");
    let Record::SegmentSeal(seal) = &mut target.value else {
        unreachable!();
    };
    seal.prefix_frame_count += 1;
    let result = check_physical_failure("seal-count", &encode(&seals), ArchiveStatus::Invalid);
    assert!(format!("{:?}", result.failure).contains("segment_seal"));
}

#[test]
fn oversized_payload_header_retains_reader_length_error_before_allocation() {
    let records = replay_fixture::fixture_frames();
    let locations = offsets(&records);
    let target = records
        .iter()
        .position(|frame| matches!(frame.value, Record::RawInput(_)))
        .expect("raw");
    let mut bytes = encode(&records);
    bytes[locations[target] + 12..locations[target] + 16]
        .copy_from_slice(&1_048_577_u32.to_le_bytes());
    let result = check_physical_failure("payload-cap", &bytes, ArchiveStatus::Corrupt);
    assert!(
        matches!(result.failure.expect("length error").kind, FailureKind::Codec(error) if error.kind == recording::CodecErrorKind::LengthError)
    );
}

#[test]
fn pinned_initial_archive_definitions_and_policy_must_match() {
    let initial = prefix_through_first_raw();
    let mut archive = initial.clone();
    let Record::ArchiveStart(start) = &mut archive[0].value else {
        unreachable!();
    };
    start.previous_archive =
        Some(domain::identity::ArchiveId::new([9; 16]).expect("nonzero archive"));
    blocked_case(
        "archive-profile",
        archive,
        RecordNo::new(1).expect("record"),
        "ProfileArchiveMismatch",
    );
    let mut spec = initial.clone();
    let target = spec
        .iter_mut()
        .find(|frame| matches!(frame.value, Record::InstrumentSpec(_)))
        .expect("spec");
    let block = target.record_no;
    let Record::InstrumentSpec(value) = &mut target.value else {
        unreachable!();
    };
    let mut fields = value.numeric.fields().clone();
    fields.price_units.quote = Token::new("OTHER").expect("unit token");
    value.numeric = NumericSpec::new(fields).expect("valid synthetic metadata");
    blocked_case(
        "instrument-profile",
        spec,
        block,
        "InstrumentDefinitionMismatch",
    );
    let mut stream = initial.clone();
    let target = stream
        .iter_mut()
        .find(|frame| matches!(frame.value, Record::StreamDefinition(_)))
        .expect("stream");
    let block = target.record_no;
    let Record::StreamDefinition(value) = &mut target.value else {
        unreachable!();
    };
    value.binding.feed_profile = FeedProfileVersion::new(2).expect("profile version");
    blocked_case("stream-profile", stream, block, "StreamDefinitionMismatch");
    let mut config = initial;
    let target = config
        .iter_mut()
        .find(|frame| matches!(frame.value, Record::ConfigDefinition(_)))
        .expect("config");
    let block = target.record_no;
    let Record::ConfigDefinition(value) = &mut target.value else {
        unreachable!();
    };
    value.fields.warmup_min_updates = Some(value.fields.warmup_min_updates.unwrap_or(0) + 1);
    blocked_case("initial-policy", config, block, "InitialConfigMismatch");
}

#[test]
fn later_configuration_blocks_projection_and_physical_suffix_stays_separate() {
    let mut records = prefix_through_first_raw();
    let mut later = records
        .iter()
        .find_map(|frame| match &frame.value {
            Record::ConfigDefinition(value) => Some(value.clone()),
            _ => None,
        })
        .expect("config");
    later.context = first_raw(&records).context;
    later.next.config = ConfigVersion::new(2).expect("config version");
    let block = records
        .last()
        .expect("raw")
        .record_no
        .checked_next()
        .expect("next");
    records.push(RecordFrame {
        record_no: block,
        segment_no: records[0].segment_no,
        value: Record::ConfigDefinition(later),
    });
    blocked_case(
        "later-config",
        records,
        block,
        "UnsupportedLaterConfigDefinition",
    );
}

#[test]
fn declared_inactive_spec_cannot_enable_unsupported_spec_activation() {
    let mut records = prefix_through_first_raw();
    records.insert(
        2,
        replay_fixture::frame(
            3,
            Record::InstrumentSpec(replay_fixture::profile::inactive_instrument_spec()),
        ),
    );
    records = replay_fixture::seal_frames(records);
    records.truncate(records.len() - 2);
    let binding = replay_fixture::profile::stream_binding();
    let control = next_control(
        &records,
        Control::SpecActivate {
            slot: binding.instrument_slot,
            expected: SpecVersion::new(1).expect("old pinned version"),
            next: SpecVersion::new(2).expect("inactive pinned version"),
        },
    );
    let block = control.record_no;
    records.push(control);
    blocked_case(
        "inactive-spec-activation",
        records,
        block,
        "UnsupportedSpecActivate",
    );
}

#[test]
fn parsed_book_and_warmup_and_freshness_proofs_never_mint_capabilities() {
    let initial = prefix_through_first_raw();
    let raw = first_raw(&initial);
    let raw_record = initial.last().expect("raw").record_no;
    let binding = initial
        .iter()
        .find_map(|frame| match &frame.value {
            Record::StreamDefinition(value) => Some(value),
            _ => None,
        })
        .expect("stream");
    let cases = [
        (
            "verification",
            "UnsupportedVerification",
            Control::Verification(VerificationEvidence {
                stream: raw.stream,
                tag: raw.tag,
                raw: raw_record,
                kind: BookEvidenceKind::Snapshot,
                profile: binding.binding.feed_profile,
                proof: binding.provenance,
            }),
        ),
        (
            "warmup",
            "UnsupportedWarmup",
            Control::Warmup(WarmupEvidence {
                stream: raw.stream,
                tag: raw.tag,
                anchor: raw_record,
                update_count: 1,
                elapsed_ns: 1,
                proof: binding.provenance,
            }),
        ),
        (
            "freshness",
            "UnsupportedFreshness",
            Control::Freshness(FreshnessEvidence {
                stream: raw.stream,
                tag: raw.tag,
                freshness: Freshness::Fresh,
                basis: Some(raw_record),
                proof: binding.provenance,
            }),
        ),
    ];
    for (label, reason, control) in cases {
        let mut records = initial.clone();
        let control = next_control(&records, control);
        let block = control.record_no;
        records.push(control);
        blocked_case(label, records, block, reason);
    }
}

#[test]
fn raw_identity_malformed_and_unsupported_payloads_stop_whole_frame_projection() {
    for (label, bytes, reason) in [
        (
            "identity",
            include_bytes!("../../../tests/fixtures/bitget/books50-snapshot.json")
                .as_slice()
                .to_vec(),
            "RawIdentityMismatch",
        ),
        ("malformed", b"{".to_vec(), "MalformedPayload"),
        (
            "rpi",
            include_bytes!("../../../tests/fixtures/bitget/rpi-books50-snapshot.json")
                .as_slice()
                .to_vec(),
            "UnsupportedPayload",
        ),
    ] {
        let mut records = prefix_through_first_raw();
        let last = records.last_mut().expect("raw");
        let block = last.record_no;
        let Record::RawInput(raw) = &mut last.value else {
            unreachable!();
        };
        raw.bytes = if label == "identity" {
            String::from_utf8(bytes)
                .expect("JSON")
                .replace("BTCUSDT", "ETHUSDT")
                .into_bytes()
        } else {
            bytes
        };
        blocked_case(label, records, block, reason);
    }
}

#[test]
fn multi_item_book_payload_is_rejected_atomically_before_pending() {
    let mut records = prefix_through_first_raw();
    let last = records.last_mut().expect("raw");
    let block = last.record_no;
    let Record::RawInput(raw) = &mut last.value else {
        unreachable!();
    };
    raw.bytes = br#"{"arg":{"instType":"usdt-futures","symbol":"BTCUSDT","topic":"books50"},"action":"snapshot","data":[{"a":[],"b":[],"pseq":0,"seq":1,"ts":"1770000000000"},{"a":[],"b":[],"pseq":0,"seq":2,"ts":"1770000000000"}],"ts":1770000000000}"#.to_vec();
    let report = blocked_case("multi-item-book", records, block, "NonHomogeneousBookFrame");
    assert!(report.contains("UnexpectedDataCount { topic: Books50, count: 2 }"));
    assert!(!report.contains("FramePending"));
}

#[test]
fn future_references_and_missing_watermark_fail_in_reader_before_proof_mapping() {
    let initial = prefix_through_first_raw();
    let raw = first_raw(&initial);
    let binding = initial
        .iter()
        .find_map(|frame| match &frame.value {
            Record::StreamDefinition(value) => Some(value),
            _ => None,
        })
        .expect("stream");
    let own_record = initial
        .last()
        .expect("raw")
        .record_no
        .checked_next()
        .expect("next");
    let invalid = [
        Control::Verification(VerificationEvidence {
            stream: raw.stream,
            tag: raw.tag,
            raw: own_record,
            kind: BookEvidenceKind::Snapshot,
            profile: binding.binding.feed_profile,
            proof: binding.provenance,
        }),
        Control::Recording(RecordingEvidence {
            health: RecordingHealth::Healthy,
            kind: WatermarkKind::Written,
            through: None,
            reason: Reason::NoFault,
        }),
    ];
    for control in invalid {
        let mut records = initial.clone();
        records.push(next_control(&records, control));
        let result = check_physical_failure(
            "invalid-reference",
            &encode(&records),
            ArchiveStatus::Invalid,
        );
        assert_eq!(
            result.last_record,
            Some(initial.last().expect("raw").record_no)
        );
    }
}

#[test]
fn unresolved_local_loss_window_preserves_unknown_quality() {
    let mut records = prefix_through_first_raw();
    let raw = first_raw(&records).clone();
    let block = records
        .last()
        .expect("raw")
        .record_no
        .checked_next()
        .expect("next");
    records.push(RecordFrame {
        record_no: block,
        segment_no: records[0].segment_no,
        value: Record::Gap(Gap {
            context: raw.context,
            scope: GapScope::ExplicitTargets(vec![GapTarget {
                stream: raw.stream,
                tag: raw.tag,
                range: None,
                loss_count: None,
            }]),
            reason: Reason::QueueOverflow,
        }),
    });
    let file = TempWal::bytes("unresolved-local-loss", &encode(&records));
    let (expected, _) = physical(&file.0);
    assert_eq!(expected.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(expected.input_quality, Some(InputQuality::Unknown));
    assert!(format!("{:?}", expected.diagnostics).contains("UnresolvedLossWindow"));
    let output = replay(&file.0);
    assert_ne!(output.status.code(), Some(0));
    assert_physical(stdout(&output), &expected);
    assert!(!stdout(&output).contains("input_quality=Some(NoKnownLoss)"));
    assert_unverified(stdout(&output));
}

#[test]
fn engineering_archive_and_record_caps_stop_dependent_admission() {
    let large = TempWal::new("archive-cap");
    let file = fs::File::create(&large.0).expect("create sparse cap fixture");
    file.set_len(8 * 1024 * 1024 + 1)
        .expect("one byte beyond archive cap");
    drop(file);
    let output = replay(&large.0);
    assert_ne!(output.status.code(), Some(0));
    assert_line(stdout(&output), "physical_status=NotScanned");
    assert_line(
        stdout(&output),
        "semantic_block_reason=Some(\"ArchiveByteCap\")",
    );
    assert_unverified(stdout(&output));

    let mut records = prefix_through_first_raw();
    let control = records
        .iter()
        .find_map(|frame| match &frame.value {
            Record::Control(value) if matches!(value.value, Control::Transport { .. }) => {
                Some(value.value.clone())
            }
            _ => None,
        })
        .expect("transport");
    while records.len() < 254 {
        records.push(next_control(&records, control.clone()));
    }
    let exact = TempWal::bytes(
        "exact-record-cap",
        &encode(&replay_fixture::seal_frames(records.clone())),
    );
    let exact_output = replay(&exact.0);
    assert_eq!(exact_output.status.code(), Some(0), "{exact_output:?}");
    assert_line(stdout(&exact_output), "physical_status=Complete");
    assert_line(stdout(&exact_output), "physical_scan_stop=None");
    assert_line(
        stdout(&exact_output),
        format!("semantic_frontier={:?}", RecordNo::new(256).ok()),
    );
    assert_unverified(stdout(&exact_output));
    while records.len() < 257 {
        records.push(next_control(&records, control.clone()));
    }
    let file = TempWal::bytes("record-cap", &encode(&replay_fixture::seal_frames(records)));
    let output = replay(&file.0);
    assert_ne!(output.status.code(), Some(0));
    assert!(stdout(&output).contains("RecordCap"), "{output:?}");
    assert_line(
        stdout(&output),
        format!("semantic_frontier={:?}", RecordNo::new(256).ok()),
    );
    assert_line(
        stdout(&output),
        format!("semantic_block_record={:?}", RecordNo::new(257).ok()),
    );
    assert_unverified(stdout(&output));
}

#[test]
fn physical_declaration_scan_stops_at_third_stream_after_earlier_profile_block() {
    let original = replay_fixture::fixture_prefix();
    let config_index = original
        .iter()
        .position(|frame| matches!(frame.value, Record::ConfigDefinition(_)))
        .expect("initial configuration");
    let mut records = original[..=config_index].to_vec();
    let definition = replay_fixture::profile::stream_definition();
    for id in [2, 3] {
        let mut next = definition.clone();
        next.context = replay_fixture::wire_context(40);
        next.binding.id = StreamId::new(id).expect("new stream");
        next.binding.channel = Channel::Trades;
        next.binding.book_id = None;
        next.binding.tag.book = None;
        records.push(replay_fixture::frame(
            u64::try_from(records.len()).expect("small count") + 1,
            Record::StreamDefinition(next),
        ));
    }
    let sealed = replay_fixture::seal_frames(records);
    let file = TempWal::bytes("declared-stream-cap", &encode(&sealed));
    let (expected, _) = physical(&file.0);
    assert_eq!(
        expected.status,
        ArchiveStatus::Complete,
        "reader accepts distinct trade streams"
    );
    let output = replay(&file.0);
    assert_ne!(output.status.code(), Some(0));
    let report = stdout(&output);
    assert_line(report, "physical_scan_stop=Some(\"DeclaredStreamCap\")");
    assert_line(
        report,
        "semantic_block_reason=Some(\"StreamDefinitionMismatch\")",
    );
    assert_line(
        report,
        format!("semantic_frontier={:?}", RecordNo::new(4).ok()),
    );
    assert_eq!(
        report
            .lines()
            .filter(|line| line.starts_with("health_snapshot="))
            .count(),
        4
    );
    assert!(!report.contains("StreamRegistered { stream: StreamId(2) }"));
    assert!(!report.contains("StreamRegistered { stream: StreamId(3) }"));
    assert!(
        !report.contains("kind=ArchiveSeal"),
        "cap must stop physical scan before suffix"
    );
    assert_unverified(report);
}
