//! W00-W20 and V2 wire assertions. All inputs are synthetic.

use domain::artifact::ArtifactError;
use domain::event::{EventError, InputContext};
use domain::identity::*;
use domain::record::*;

use crate::support::accounting::LossError;
use crate::support::binary::{Crc32, Error, ErrorKind, Reader, crc32};
use crate::support::binary_fixtures::{self as frozen, golden, sequence};
use crate::support::fixtures;
use crate::support::health::{BookValidity, Fault, ModelError};
use crate::support::scenario::Scenario;
use crate::support::wal::{MAX_PAYLOAD, decode_exact, encode_frame, scan_frame};
use crate::support::wal_apply::apply_exact;
use crate::support::wal_expected as expected;
use crate::support::wal_recovery::{Completion, RecoveryFault, framing_prefix, recover};

fn repair_crc(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let checksum = crc32(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum.to_le_bytes());
}

fn encoded(frames: &[RecordFrame]) -> Vec<u8> {
    frames.iter().flat_map(|frame| encode_frame(frame).unwrap()).collect()
}

#[test]
fn w00_crc_independent_known_vectors_and_streaming() {
    assert_eq!(crc32(b""), 0);
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    let mut state = Crc32::default();
    state.update(b"1234");
    state.update(b"56789");
    assert_eq!(state.digest(), 0xcbf4_3926);
    let start = golden("W01");
    assert_eq!(crc32(&start[..70]), 0x9e02_c413);
    assert_eq!(crc32(&start), 0x2144_df1c);
}

#[test]
fn w01_encoder_and_decoder_match_independent_start_golden() {
    let bytes = golden("W01");
    let value = expected::start();
    assert_eq!(encode_frame(&value).unwrap(), bytes);
    assert_eq!(decode_exact(&bytes, false, &Default::default()).unwrap(), value);
    let view = scan_frame(&bytes, 0).unwrap();
    assert_eq!((view.length, view.payload.len(), view.checksum), (74, 38, 0x9e02_c413));
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.completion, Completion::ValidPrefixIncomplete);
    assert_eq!(result.last_good_offset, 74);
    assert_eq!(result.last_record, Some(fixtures::record(1)));
    assert!(result.failure.is_none());
}

#[test]
fn w02_full_single_golden_exact_dtos_seals_and_quality() {
    let bytes = sequence(frozen::SINGLE);
    let expected = expected::single();
    assert_eq!(encoded(&expected), bytes);
    assert_eq!(bytes.len(), 1296);
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.failure, None);
    assert_eq!(result.frames, expected);
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(result.input_quality, Some(InputQuality::GapsRecorded));
    assert_eq!((result.last_good_offset, result.framing_good_offset), (1296, 1296));
    assert_eq!(result.last_record, Some(fixtures::record(11)));
    assert!(result.steps.iter().all(|step| step.effects.is_empty()));
    let model = result.model.unwrap();
    let stream = &model.streams[&StreamId::new(1).unwrap()];
    assert_eq!(stream.loss.accounted_frontier, 3);
    assert_eq!(stream.loss.window, None);
    assert_eq!(stream.barrier, 7);
    assert_eq!(stream.book, Some(BookValidity::Invalid(Fault::Gap(Reason::QueueOverflow))));
    assert_eq!(stream.freshness, Freshness::Unknown);
    assert_eq!(stream.pending.len(), 1);
    assert_eq!(stream.pending[0].raw, fixtures::raw_id(8));
    assert_eq!(stream.last_event_cursor, None);
    assert_eq!(stream.candidate, None);
    assert!(!model.usable_data(stream.binding.id));
}

#[test]
fn w02_full_multi_segment_golden_chain_and_inherited_state() {
    let first = sequence(frozen::MULTI0);
    let second = sequence(frozen::MULTI1);
    let (expected0, expected1) = expected::multi();
    assert_eq!(encoded(&expected0), first);
    assert_eq!(encoded(&expected1), second);
    assert_eq!((first.len(), second.len()), (1030, 421));
    let result = recover(&[&first, &second], frozen::environment());
    assert_eq!(result.failure, None);
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(result.frames, [expected0, expected1].concat());
    assert_eq!(result.last_good_offset, 1451);
    assert_eq!(result.last_record, Some(fixtures::record(13)));
    let stream = &result.model.as_ref().unwrap().streams[&StreamId::new(1).unwrap()];
    assert_eq!(stream.loss.accounted_frontier, 3);
    assert_eq!(stream.pending[0].raw, fixtures::raw_id(10));
    assert_eq!(stream.last_event_cursor, None);
}

#[test]
fn w08_every_start_byte_cut_is_incomplete_at_zero() {
    let bytes = golden("W01");
    for cut in 0..bytes.len() {
        let result = recover(&[&bytes[..cut]], frozen::environment());
        assert_eq!(result.last_good_offset, 0, "cut={cut}");
        assert_eq!(result.last_record, None);
        assert_eq!(result.completion, if cut == 0 { Completion::NoArchive } else { Completion::TruncatedTail });
    }
}

#[test]
fn w09_every_byte_cut_of_single_archive_retains_exact_boundary() {
    let bytes = sequence(frozen::SINGLE);
    let ends = [74_usize, 299, 482, 650, 724, 840, 964, 1080, 1161, 1227, 1296];
    for cut in 0..bytes.len() {
        let result = recover(&[&bytes[..cut]], frozen::environment());
        let accepted = ends.iter().filter(|end| **end <= cut).count();
        let boundary = accepted.checked_sub(1).map_or(0, |i| ends[i]);
        assert_eq!(result.last_good_offset, boundary as u64, "cut={cut}");
        assert_eq!(result.frames.len(), accepted, "cut={cut}");
        assert_eq!(result.last_record, (accepted > 0).then(|| fixtures::record(accepted as u64)));
        let completion = if cut == 0 {
            Completion::NoArchive
        } else if cut != boundary {
            Completion::TruncatedTail
        } else if cut == 1227 {
            Completion::SegmentSealedArchiveIncomplete
        } else {
            Completion::ValidPrefixIncomplete
        };
        assert_eq!(result.completion, completion, "cut={cut}");
    }
}

#[test]
fn w09_every_byte_cut_across_segment_boundary_retains_prefix() {
    let first = sequence(frozen::MULTI0);
    let second = sequence(frozen::MULTI1);
    let ends0 = [74_usize, 299, 482, 650, 724, 840, 964, 1030];
    let ends1 = [88_usize, 205, 286, 352, 421];
    for cut in 0..first.len() {
        let result = recover(&[&first[..cut]], frozen::environment());
        let boundary = ends0.iter().copied().filter(|end| *end <= cut).max().unwrap_or(0);
        assert_eq!(result.last_good_offset, boundary as u64, "segment0 cut={cut}");
        assert_ne!(result.completion, Completion::Complete);
    }
    let missing_second = recover(&[&first], frozen::environment());
    assert_eq!(missing_second.completion, Completion::SegmentSealedArchiveIncomplete);
    assert_eq!(missing_second.last_good_offset, 1030);
    for cut in 1..second.len() {
        let result = recover(&[&first, &second[..cut]], frozen::environment());
        let accepted = ends1.iter().filter(|end| **end <= cut).count();
        let boundary = accepted.checked_sub(1).map_or(0, |i| ends1[i]);
        assert_eq!(result.last_good_offset, 1030 + boundary as u64, "segment1 cut={cut}");
        assert_eq!(result.last_record, Some(fixtures::record(8 + accepted as u64)));
        let completion = if cut != boundary {
            Completion::TruncatedTail
        } else if cut == 352 {
            Completion::SegmentSealedArchiveIncomplete
        } else {
            Completion::ValidPrefixIncomplete
        };
        assert_eq!(result.completion, completion, "segment1 cut={cut}");
    }
}

#[test]
fn w03_unknown_header_control_and_encoding_tags_report_exact_offsets() {
    for (offset, field, value) in [(4, "frame_version", 2_u16), (6, "record_schema_version", 2), (8, "record_kind", 65535)] {
        let mut bytes = golden("W01");
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        repair_crc(&mut bytes);
        assert_eq!(scan_frame(&bytes, 0), Err(Error::new(offset, ErrorKind::Unsupported { field, value: value.into() })));
    }
    for (name, offset, field) in [("UP5", 56, "control_tag"), ("RAW6", 97, "RawInput.payload_encoding")] {
        let mut bytes = golden(name);
        bytes[offset] = 255;
        repair_crc(&mut bytes);
        assert_eq!(decode_exact(&bytes, true, &Default::default()), Err(Error::new(offset, ErrorKind::Unsupported { field, value: 255 })));
    }
}

#[test]
fn w04_magic_flags_reserved_options_bools_tokens_and_trailing_bytes() {
    for (offset, field) in [(0, "magic"), (10, "flags"), (28, "reserved")] {
        let mut bytes = golden("W01");
        bytes[offset] ^= 1;
        repair_crc(&mut bytes);
        assert_eq!(scan_frame(&bytes, 0), Err(Error::new(offset, ErrorKind::Corrupt(field))));
    }
    for (name, offset, field) in [("W01", 69, "option"), ("SEAL10", 60, "bool")] {
        let mut bytes = golden(name);
        bytes[offset] = 2;
        repair_crc(&mut bytes);
        assert_eq!(decode_exact(&bytes, false, &Default::default()), Err(Error::new(offset, ErrorKind::InvalidPayload(field))));
    }
    let mut token = golden("SPEC2");
    token[65] = b' ';
    repair_crc(&mut token);
    assert_eq!(decode_exact(&token, false, &Default::default()), Err(Error::new(64, ErrorKind::Identity(IdentityError::InvalidToken))));
    let mut extra = golden("W01");
    extra.insert(70, 0);
    extra[12..16].copy_from_slice(&39_u32.to_le_bytes());
    repair_crc(&mut extra);
    assert_eq!(decode_exact(&extra, false, &Default::default()), Err(Error::new(70, ErrorKind::InvalidPayload("trailing_bytes"))));
}

#[test]
fn w06_w07_caps_and_checked_offsets_precede_allocation() {
    for length in [MAX_PAYLOAD as u32 + 1, u32::MAX] {
        let mut bytes = golden("W01");
        bytes[12..16].copy_from_slice(&length.to_le_bytes());
        assert_eq!(scan_frame(&bytes, 0), Err(Error::new(12, ErrorKind::LengthError)));
    }
    assert_eq!(scan_frame(&golden("W01"), u64::MAX - 73), Err(Error::new(12, ErrorKind::LengthError)));
    assert_eq!(Reader::new(&[1], usize::MAX).err(), Some(Error::new(usize::MAX, ErrorKind::LengthError)));
    assert_eq!(Reader::new(&[], 0).unwrap().count(usize::MAX, 2, usize::MAX), Err(Error::new(0, ErrorKind::LengthError)));
    let mut raw = golden("RAW6");
    raw[98..102].copy_from_slice(&u32::MAX.to_le_bytes());
    repair_crc(&mut raw);
    assert_eq!(decode_exact(&raw, true, &Default::default()), Err(Error::new(98, ErrorKind::LengthError)));
    let mut gap = golden("V2-GAP-GOOD");
    gap[58..60].copy_from_slice(&2_u16.to_le_bytes());
    repair_crc(&mut gap);
    assert_eq!(decode_exact(&gap, true, &Default::default()), Err(Error::new(60, ErrorKind::LengthError)));
}

#[test]
fn w05_w10_middle_corruption_does_not_scan_for_following_magic() {
    for frame_byte in [102_usize, 112] {
        let mut bytes = sequence(frozen::SINGLE);
        bytes[724 + frame_byte] ^= 1;
        let result = recover(&[&bytes], frozen::environment());
        assert_eq!(result.last_good_offset, 724);
        assert_eq!(result.last_record, Some(fixtures::record(5)));
        assert_eq!(result.failure.unwrap().fault, RecoveryFault::Binary(Error::new(112, ErrorKind::ChecksumMismatch)));
        assert_eq!(result.frames.len(), 5);
    }
}

#[test]
fn w11_whole_raw_final_seal_or_segment_loss_never_means_complete() {
    let full = sequence(frozen::SINGLE);
    let no_archive_seal = recover(&[&full[..1227]], frozen::environment());
    assert_eq!(no_archive_seal.completion, Completion::SegmentSealedArchiveIncomplete);
    let no_segment_seal = [full[..1161].to_vec(), full[1227..].to_vec()].concat();
    let result = recover(&[&no_segment_seal], frozen::environment());
    assert_eq!(result.last_good_offset, 1161);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
    let no_raw = [full[..964].to_vec(), full[1080..].to_vec()].concat();
    let result = recover(&[&no_raw], frozen::environment());
    assert_eq!(result.last_good_offset, 964);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
    let first = sequence(frozen::MULTI0);
    assert_eq!(recover(&[&first], frozen::environment()).completion, Completion::SegmentSealedArchiveIncomplete);
}

#[test]
fn w12_w20_forged_seal_fields_and_wrong_aggregate_exclude_no_checks() {
    for (start, field_offset) in [(1161, 32), (1161, 40), (1161, 48), (1161, 52), (1227, 32), (1227, 36), (1227, 44), (1227, 52), (1227, 56)] {
        let mut bytes = sequence(frozen::SINGLE);
        let length = scan_frame(&bytes[start..], start as u64).unwrap().length;
        bytes[start + field_offset] ^= 1;
        repair_crc(&mut bytes[start..start + length]);
        let result = recover(&[&bytes], frozen::environment());
        assert_eq!(result.last_good_offset, start as u64);
        assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
    }
    let mut bytes = sequence(frozen::SINGLE);
    let wrong = crc32(&bytes[..1161]);
    bytes[1161 + 48..1161 + 52].copy_from_slice(&wrong.to_le_bytes());
    repair_crc(&mut bytes[1161..1227]);
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
    assert_eq!(result.last_good_offset, 1161);
}

#[test]
fn w13_wrong_segment_link_or_detached_segment_fails_at_boundary() {
    let first = sequence(frozen::MULTI0);
    for offset in [24_usize, 32, 48, 64, 68, 72, 80] {
        let mut second = sequence(frozen::MULTI1);
        if offset == 64 {
            second[64..68].copy_from_slice(&2_u32.to_le_bytes());
        } else {
            second[offset] ^= 1;
        }
        repair_crc(&mut second[..88]);
        let result = recover(&[&first, &second], frozen::environment());
        assert_eq!(result.last_good_offset, 1030, "offset={offset}");
        assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError, "offset={offset}");
    }
    let second = sequence(frozen::MULTI1);
    let result = recover(&[&second], frozen::environment());
    assert_eq!(result.last_good_offset, 0);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
}

#[test]
fn w14_trailing_bytes_after_archive_seal_are_not_ignored() {
    let mut bytes = sequence(frozen::SINGLE);
    bytes.extend_from_slice(b"PSRW");
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.last_good_offset, 1296);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::TrailingDataError);
    assert_eq!(result.completion, Completion::Invalid);
}

#[test]
fn w15_unresolved_local_loss_requires_unknown_quality_even_when_sealed() {
    let bytes = sequence(frozen::OPEN);
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.failure, None);
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(result.input_quality, Some(InputQuality::Unknown));
    assert_eq!(result.last_good_offset, 1075);
    assert_eq!(result.unresolved_loss_streams, vec![StreamId::new(1).unwrap()]);
    let stream = &result.model.as_ref().unwrap().streams[&StreamId::new(1).unwrap()];
    assert_eq!(stream.loss.accounted_frontier, 1);
    assert_eq!(stream.loss.window.as_ref().unwrap().recorded_count, None);
    for quality in [1, 2] {
        let mut bad = bytes.clone();
        bad[1006 + 64] = quality;
        repair_crc(&mut bad[1006..]);
        let result = recover(&[&bad], frozen::environment());
        assert_eq!(result.last_good_offset, 1006);
        assert_eq!(result.failure.unwrap().fault, RecoveryFault::OrderOrChainError);
    }
}

#[test]
fn w16_local_gap_covers_attempts_but_source_gap_does_not() {
    let prefix = sequence(&frozen::SINGLE[..6]);
    let mut source = expected::gap(7, false);
    if let Record::Gap(gap) = &mut source.value { gap.reason = Reason::SourceGap; }
    let bytes = [prefix, encode_frame(&source).unwrap(), golden("RAW8")].concat();
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.last_record, Some(fixtures::record(7)));
    assert_eq!(result.last_good_offset, 940);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::Semantic(ModelError::Loss(LossError::UnaccountedAttemptGap)));
    let stream = &result.model.as_ref().unwrap().streams[&StreamId::new(1).unwrap()];
    assert_eq!(stream.loss.accounted_frontier, 1);
    assert_eq!(stream.loss.window, None);
    assert_eq!(stream.barrier, 7);
    assert_eq!(stream.book, Some(BookValidity::Invalid(Fault::Gap(Reason::SourceGap))));
}

#[test]
fn artifact_missing_blocks_semantics_without_claiming_framing_is_replay() {
    let bytes = sequence(frozen::SINGLE);
    let mut env = frozen::environment();
    env.resolver.supplied.remove(&frozen::original("AF-I1").reference);
    let result = recover(&[&bytes], env);
    assert_eq!(result.last_good_offset, 74);
    assert_eq!(result.framing_good_offset, 299);
    assert_eq!(result.last_record, Some(fixtures::record(1)));
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::Semantic(ModelError::Artifact(ArtifactError::MissingArtifact)));
    assert_eq!(framing_prefix(&[&bytes]).unwrap(), 1296);
    assert_ne!(result.completion, Completion::Complete);
}

#[test]
fn v2_gap_order_bytes_and_exact_decode_before_transition() {
    let bytes = golden("V2-GAP-GOOD");
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[56..60], &[1, 4, 1, 0]);
    assert_eq!(encode_frame(&expected::gap(11, false)).unwrap(), bytes);
    assert_eq!(decode_exact(&bytes, true, &Default::default()).unwrap(), expected::gap(11, false));
    let mut scenario = Scenario::initial(fixtures::policy());
    scenario.raw(vec![fixtures::snapshot()], 10).unwrap();
    let effects = apply_exact(&bytes, &mut scenario.model, &mut scenario.env).unwrap();
    assert!(effects.effects.is_empty());
    assert!(effects.candidates_created.is_empty());
    assert_eq!(scenario.model.last_record, fixtures::record(11));
    assert_eq!(scenario.model.evaluation_ns, 11);
    assert_eq!(scenario.stream().barrier, 11);
    assert_eq!(scenario.stream().book, Some(BookValidity::Invalid(Fault::Gap(Reason::QueueOverflow))));
    assert_eq!(scenario.stream().freshness, Freshness::Unknown);
    assert_eq!(scenario.model.recording.health, RecordingHealth::Degraded);
    assert!(scenario.stream().pending.is_empty());
    assert_eq!(scenario.stream().anchor, None);
    assert_eq!(scenario.stream().witness, None);
    assert_eq!(scenario.stream().last_valid_sample_ns, None);
    assert_eq!(scenario.stream().last_event_cursor, None);
    assert_eq!(scenario.stream().progress, 0);
    let loss = &scenario.stream().loss;
    assert_eq!(loss.accounted_frontier, 1);
    let window = loss.window.as_ref().unwrap();
    assert_eq!((window.gap_record, window.left, window.tag, window.recorded_count), (fixtures::record(11), 1, scenario.stream().binding.tag, None));
}

#[test]
fn v2_gap_reversed_has_own_valid_crc_and_preserves_prefix_and_state() {
    let bytes = golden("V2-GAP-BAD");
    assert_eq!(&bytes[56..60], &[4, 1, 1, 0]);
    assert_eq!(scan_frame(&bytes, 0).unwrap().checksum, 0x35ed_ebf2);
    let expected_error = Error::new(56, ErrorKind::Unsupported { field: "Gap.scope_kind", value: 4 });
    assert_eq!(decode_exact(&bytes, true, &Default::default()), Err(expected_error.clone()));
    let mut scenario = Scenario::initial(fixtures::policy());
    scenario.raw(vec![fixtures::snapshot()], 10).unwrap();
    let mut retained = scenario.model.clone();
    let record_count = scenario.env.prefix.records.len();
    retained.blocked = Some(ModelError::Record(RecordError::Unsupported { field: "Gap.scope_kind", value: 4 }));
    assert_eq!(apply_exact(&bytes, &mut scenario.model, &mut scenario.env), Err(RecoveryFault::Binary(expected_error)));
    assert_eq!(scenario.model, retained);
    assert_eq!(scenario.env.prefix.records.len(), record_count);
    assert_eq!(scenario.model.last_record, fixtures::record(10));
    assert_eq!(scenario.stream().pending[0].raw, fixtures::raw_id(10));
}

#[test]
fn context_bootstrap_and_definition_references_are_not_guessed() {
    let mut raw = golden("RAW6");
    raw[48..56].fill(0);
    repair_crc(&mut raw);
    assert_eq!(decode_exact(&raw, false, &Default::default()), Err(Error::new(48, ErrorKind::Event(EventError::InvalidBootstrapContext))));
    assert_eq!(decode_exact(&golden("STREAM3"), false, &Default::default()), Err(Error::new(64, ErrorKind::UnknownDefinition)));
    let mut bytes = sequence(frozen::SINGLE);
    bytes[482 + 48..482 + 56].copy_from_slice(&[1, 0, 0, 0, 1, 0, 0, 0]);
    repair_crc(&mut bytes[482..650]);
    let result = recover(&[&bytes], frozen::environment());
    assert_eq!(result.last_good_offset, 482);
    assert_eq!(result.failure.unwrap().fault, RecoveryFault::Semantic(ModelError::Event(EventError::ContextMismatch)));
    assert_eq!(result.model.unwrap().timeline.active().map(InputContext::Active), None);
}
