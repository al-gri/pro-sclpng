//! V2 policy tags/mirrors and the unchanged nine-cell A2 mode/gate matrix.

use crate::support::fixtures::policy;
use domain::policy::*;

#[test]
fn v2_policy_silence_all_and_unsupported() {
    for byte in 0..=u8::MAX {
        let expected = match byte {
            1 => Ok(SilenceRule::UnknownOnSilence),
            2 => Ok(SilenceRule::StaleAfterDeadline),
            value => Err(PolicyError::Unsupported { field: "Config.silence_rule", value }),
        };
        assert_eq!(SilenceRule::try_from(byte), expected);
    }
    assert_eq!(SilenceRule::UnknownOnSilence.tag(), 1);
    assert_eq!(SilenceRule::StaleAfterDeadline.tag(), 2);
}

#[test]
fn v2_policy_gate_all_and_unsupported() {
    for byte in 0..=u8::MAX {
        let expected = match byte {
            1 => Ok(RecordingGate::Written),
            2 => Ok(RecordingGate::Flushed),
            3 => Ok(RecordingGate::Durable),
            value => Err(PolicyError::Unsupported { field: "Config.recording_gate", value }),
        };
        assert_eq!(RecordingGate::try_from(byte), expected);
    }
    assert_eq!(RecordingGate::Written.tag(), 1);
    assert_eq!(RecordingGate::Flushed.tag(), 2);
    assert_eq!(RecordingGate::Durable.tag(), 3);
}

#[test]
fn v_r2_mode_gate_all_nine_cells() {
    use DurabilityMode::{Buffered, GroupSynced, SyncBeforePublish};
    use RecordingGate::{Durable, Flushed, Written};
    let cells = [
        (Buffered, Written, true),
        (Buffered, Flushed, true),
        (Buffered, Durable, true),
        (GroupSynced, Written, false),
        (GroupSynced, Flushed, false),
        (GroupSynced, Durable, true),
        (SyncBeforePublish, Written, false),
        (SyncBeforePublish, Flushed, false),
        (SyncBeforePublish, Durable, true),
    ];
    for (mode, gate, valid) in cells {
        let expected = if valid {
            Ok(())
        } else {
            Err(PolicyError::InvalidConfiguration { field: "Config.recording_gate" })
        };
        assert_eq!(mode.validate_gate(gate), expected);
    }
}

#[test]
fn v2_policy_no_ordinal_cast_between_gate_and_watermark() {
    let gate = RecordingGate::try_from(3).unwrap();
    let watermark = WatermarkKind::try_from(3).unwrap();
    assert_eq!(gate, RecordingGate::Durable);
    assert_eq!(watermark, WatermarkKind::Written);
    assert_eq!(watermark.achieved_gate(), Some(RecordingGate::Written));
    assert!(!watermark.achieved_gate().unwrap().covers(gate));
    assert_eq!(WatermarkKind::Accepted.achieved_gate(), None);
    assert_eq!(WatermarkKind::Appended.achieved_gate(), None);
    assert!(RecordingGate::Durable.covers(RecordingGate::Flushed));
    assert!(RecordingGate::Flushed.covers(RecordingGate::Written));
    assert!(!RecordingGate::Written.covers(RecordingGate::Flushed));
    for (tag, value) in [
        (1, WatermarkKind::Accepted), (2, WatermarkKind::Appended),
        (3, WatermarkKind::Written), (4, WatermarkKind::Flushed), (5, WatermarkKind::Durable),
    ] {
        assert_eq!(WatermarkKind::try_from(tag), Ok(value));
        assert_eq!(value.tag(), tag);
    }
    for tag in [0, 6, 255] {
        assert_eq!(WatermarkKind::try_from(tag), Err(PolicyError::Unsupported { field: "RecordingEvidence.watermark_kind", value: tag }));
    }
}

#[test]
fn v2_policy_wal_psad_match_and_supported_mismatch() {
    for silence in 1..=2 {
        for gate in 1..=3 {
            let mut wire = policy().fields;
            wire.silence_rule = SilenceRule::try_from(silence).unwrap();
            wire.recording_gate = RecordingGate::try_from(gate).unwrap();
            assert_eq!(wire.validate(DurabilityMode::Buffered), Ok(()));
            assert_eq!(wire.validate_mirror(wire), Ok(()));
        }
    }
    let wire = policy().fields;
    let mut body = wire;
    body.silence_rule = SilenceRule::StaleAfterDeadline;
    assert_eq!(wire.validate_mirror(body), Err(PolicyError::InvalidPayload {
        field: "Config.silence_rule", detail: "PolicyRepresentationMismatch",
    }));
    body = wire;
    body.recording_gate = RecordingGate::Written;
    assert_eq!(wire.validate_mirror(body), Err(PolicyError::InvalidPayload {
        field: "Config.recording_gate", detail: "PolicyRepresentationMismatch",
    }));
    assert_eq!(wire, policy().fields);
}

#[test]
fn policy_thresholds_pending_bounds_and_none_semantics() {
    let valid = policy();
    assert_eq!(valid.validate(DurabilityMode::SyncBeforePublish), Ok(()));
    let mut p = valid;
    p.fields.freshness_deadline_ns = None;
    assert_eq!(p.validate(DurabilityMode::SyncBeforePublish), Ok(()));
    p.fields.silence_rule = SilenceRule::StaleAfterDeadline;
    assert_eq!(p.validate(DurabilityMode::SyncBeforePublish), Err(PolicyError::InvalidConfiguration { field: "Config.freshness_deadline_ns" }));
    p = valid;
    p.fields.warmup_min_updates = None;
    p.fields.warmup_min_elapsed_ns = None;
    assert_eq!(p.validate(DurabilityMode::SyncBeforePublish), Err(PolicyError::InvalidConfiguration { field: "Config.warmup_thresholds" }));
    for (frames, bytes, outputs, wait, field) in [
        (0, 256, 4, 50, "Config.pending_max_frames"),
        (257, 256, 4, 50, "Config.pending_max_frames"),
        (2, 0, 4, 50, "Config.pending_max_raw_bytes"),
        (2, 16_777_217, 4, 50, "Config.pending_max_raw_bytes"),
        (2, 256, 0, 50, "Config.pending_max_outputs"),
        (2, 256, 65_537, 50, "Config.pending_max_outputs"),
        (2, 256, 4, 0, "Config.pending_wait_ns"),
    ] {
        p = valid;
        p.pending_max_frames = frames;
        p.pending_max_raw_bytes = bytes;
        p.pending_max_outputs = outputs;
        p.pending_wait_ns = wait;
        assert_eq!(p.validate(DurabilityMode::Buffered), Err(PolicyError::InvalidConfiguration { field }));
    }
    p = valid;
    p.fields.allow_quiet_with_proof = true;
    assert_eq!(p.validate(DurabilityMode::Buffered), Err(PolicyError::InvalidConfiguration { field: "Config.quiet_max_lifetime_ns" }));
    p.quiet_max_lifetime_ns = Some(10);
    assert_eq!(p.validate(DurabilityMode::Buffered), Ok(()));
}
