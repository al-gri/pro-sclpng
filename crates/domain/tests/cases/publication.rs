//! A2 finite candidate/fence traces with explicitly synthetic completion.

use crate::support::fixtures::{self, record};
use crate::support::health::ModelError;
use crate::support::publication::*;
use crate::support::scenario::Scenario;
use domain::artifact::ArtifactError;
use domain::identity::{ArchiveId, CaptureSessionId};
use domain::policy::*;
use domain::record::*;

fn at_candidate21() -> Scenario {
    let mut s = Scenario::recovered();
    for n in 15..=20 {
        s.timer(n).unwrap();
    }
    let out = s
        .receipt(
            RecordingHealth::Healthy,
            WatermarkKind::Durable,
            Some(20),
            21,
        )
        .unwrap();
    assert!(out.effects.is_empty());
    assert_eq!(out.candidates_created.len(), 1);
    assert_eq!(out.candidates_created[0].creation_record.get(), 21);
    s
}

fn completion() -> SyntheticCompletion {
    SyntheticCompletion {
        archive: fixtures::archive(),
        session: fixtures::clock().session,
        gate: RecordingGate::Durable,
        through: Some(record(21)),
        known_achieved_prefix: Some(record(21)),
        known_accepted_prefix: Some(record(21)),
        previous_reported_prefix: Some(record(20)),
        continuous_prefix: true,
        explicitly_attested: true,
    }
}

#[test]
fn v_r2_finite_receipt_then_final_fence_includes_receipt_in_basis() {
    let s = at_candidate21();
    let candidate = s.stream().candidate.as_ref().unwrap();
    let before = s.model.clone();
    assert_eq!(candidate.id.creation_record.get(), 21);
    assert_eq!(candidate.causal_frontier.get(), 21);
    assert_eq!(candidate.available_at.record.get(), 21);
    assert_eq!(
        candidate.projection.recording.last_receipt,
        Some(record(21))
    );
    assert_eq!(
        candidate.projection.recording.watermarks.durable,
        Some(record(20))
    );
    assert_eq!(
        s.model.permit(candidate, None),
        Err(PermitError::FenceMissing)
    );
    assert_eq!(s.model.permit(candidate, Some(&completion())), Ok(()));
    assert_eq!(s.model, before);
    assert_eq!(s.model.last_record.get(), 21);
}

#[test]
fn v_r2_missing_behind_weak_wrong_scope_future_and_unverified_fences() {
    let s = at_candidate21();
    let candidate = s.stream().candidate.as_ref().unwrap();
    let original = completion();
    let mut variants = Vec::new();
    let mut value = original.clone();
    value.through = Some(record(20));
    variants.push((value, PermitError::FenceBehindCandidate));
    let mut value = original.clone();
    value.gate = RecordingGate::Written;
    variants.push((value, PermitError::FenceTooWeak));
    let mut value = original.clone();
    value.archive = ArchiveId::new([3; 16]).unwrap();
    variants.push((value, PermitError::FenceScopeMismatch));
    let mut value = original.clone();
    value.session = CaptureSessionId::new([4; 16]).unwrap();
    variants.push((value, PermitError::FenceScopeMismatch));
    let mut value = original.clone();
    value.through = Some(record(22));
    variants.push((value, PermitError::FenceBeyondAchieved));
    let mut value = original.clone();
    value.known_achieved_prefix = None;
    variants.push((value, PermitError::UnverifiedStorageCompletion));
    let mut value = original.clone();
    value.known_accepted_prefix = None;
    variants.push((value, PermitError::UnverifiedStorageCompletion));
    let mut value = original.clone();
    value.explicitly_attested = false;
    variants.push((value, PermitError::UnverifiedStorageCompletion));
    let mut value = original.clone();
    value.continuous_prefix = false;
    variants.push((value, PermitError::UnverifiedStorageCompletion));
    let mut value = original;
    value.previous_reported_prefix = Some(record(22));
    variants.push((value, PermitError::WatermarkRegression));
    for (fence, expected) in variants {
        assert_eq!(s.model.permit(candidate, Some(&fence)), Err(expected));
        assert_eq!(candidate.causal_frontier.get(), 21);
        assert_eq!(s.model.last_record.get(), 21);
    }
}

#[test]
fn v_r2_self_future_none_and_regressing_receipts_retain_prefix() {
    for (through, expected) in [
        (Some(21), ReceiptError::InvalidAcknowledgement),
        (Some(22), ReceiptError::InvalidAcknowledgement),
        (None, ReceiptError::MissingWatermark),
    ] {
        let mut s = Scenario::recovered();
        for n in 15..=20 {
            s.timer(n).unwrap();
        }
        let before = s.model.clone();
        assert_eq!(
            s.receipt(
                RecordingHealth::Healthy,
                WatermarkKind::Durable,
                through,
                21
            ),
            Err(ModelError::Receipt(expected))
        );
        assert_eq!(s.model.last_record.get(), 20);
        assert_eq!(s.model.evaluation_ns, 20);
        assert_eq!(s.model.streams, before.streams);
        assert_eq!(s.model.recording, before.recording);
    }
    let mut s = at_candidate21();
    let before = s.model.clone();
    assert_eq!(
        s.receipt(
            RecordingHealth::Healthy,
            WatermarkKind::Durable,
            Some(19),
            22
        ),
        Err(ModelError::Receipt(ReceiptError::WatermarkRegression))
    );
    assert_eq!(s.model.last_record.get(), 21);
    assert_eq!(s.model.recording, before.recording);
}

#[test]
fn v_r2_recording_order_partial_failure_and_weaker_evidence() {
    let mut marks = Watermarks::default();
    marks
        .observe(
            record(21),
            record(20),
            &RecordingEvidence {
                health: RecordingHealth::Healthy,
                kind: WatermarkKind::Written,
                through: Some(record(20)),
                reason: Reason::NoFault,
            },
        )
        .unwrap();
    assert_eq!(marks.written, Some(record(20)));
    assert_eq!(marks.flushed, None);
    assert_eq!(marks.durable, None);
    let before = marks.clone();
    marks
        .observe(
            record(22),
            record(21),
            &RecordingEvidence {
                health: RecordingHealth::Failed,
                kind: WatermarkKind::Durable,
                through: None,
                reason: Reason::WriteFailure,
            },
        )
        .unwrap();
    assert_eq!(marks, before);
    let mut impossible = before;
    impossible.durable = Some(record(20));
    assert_eq!(
        impossible.validate(),
        Err(ReceiptError::WatermarkOrderError)
    );
    let mut marks = Watermarks::default();
    assert_eq!(
        marks.observe(
            record(21),
            record(19),
            &RecordingEvidence {
                health: RecordingHealth::Healthy,
                kind: WatermarkKind::Durable,
                through: Some(record(20)),
                reason: Reason::NoFault,
            }
        ),
        Err(ReceiptError::WatermarkOrderError)
    );
    assert_eq!(marks, Watermarks::default());
}

#[test]
fn v_r2_revoked_candidates_cannot_be_resurrected_by_late_fence() {
    for reason in 0..3 {
        let mut s = at_candidate21();
        let candidate = s.stream().candidate.clone().unwrap();
        match reason {
            0 => {
                s.gap(Reason::SourceGap, None, None, 22).unwrap();
            }
            1 => {
                s.transport(Transport::Down, 22).unwrap();
            }
            _ => {
                s.receipt(RecordingHealth::Failed, WatermarkKind::Durable, None, 22)
                    .unwrap();
            }
        }
        assert_eq!(
            s.model.permit(&candidate, Some(&completion())),
            Err(PermitError::CandidateRevoked)
        );
        assert!(s.stream().candidate.as_ref().unwrap().revoked);
        assert_eq!(s.model.last_record.get(), 22);
    }
}

#[test]
fn v_r2_superseded_candidate_requires_new_causal_frontier() {
    let mut s = at_candidate21();
    let old = s.stream().candidate.clone().unwrap();
    s.timer(22).unwrap();
    let new = s.stream().candidate.as_ref().unwrap();
    assert_eq!(new.id.creation_record.get(), 22);
    assert_eq!(new.causal_frontier.get(), 22);
    assert_eq!(
        s.model.permit(new, Some(&completion())),
        Err(PermitError::FenceBehindCandidate)
    );
    assert_eq!(
        s.model.permit(&old, Some(&completion())),
        Err(PermitError::CandidateRevoked)
    );
    let mut fence = completion();
    fence.through = Some(record(22));
    fence.known_achieved_prefix = Some(record(22));
    fence.known_accepted_prefix = Some(record(22));
    assert_eq!(s.model.permit(new, Some(&fence)), Ok(()));
}

#[test]
fn v_r2_config_downgrade_cannot_weaken_immutable_mode() {
    for mode in [
        DurabilityMode::GroupSynced,
        DurabilityMode::SyncBeforePublish,
    ] {
        let mut s = at_candidate21();
        s.model.start.mode = mode;
        let mut policy = fixtures::policy();
        policy.fields.recording_gate = RecordingGate::Written;
        let before = s.model.clone();
        assert_eq!(
            s.config_change(2, 1, policy, 22),
            Err(ModelError::Policy(PolicyError::InvalidConfiguration {
                field: "Config.recording_gate",
            }))
        );
        assert_eq!(s.model.start.mode, mode);
        assert_eq!(s.model.last_record.get(), 21);
        assert_eq!(s.model.streams, before.streams);
        assert_eq!(s.model.context().unwrap().config.get(), 1);
    }
}

#[test]
fn v_r2_missing_artifact_blocks_even_an_earlier_waiting_candidate() {
    let mut s = at_candidate21();
    let candidate = s.stream().candidate.clone().unwrap();
    let reference = s.stream().profile_ref;
    s.env.resolver.supplied.remove(&reference);
    assert_eq!(
        s.timer(22),
        Err(ModelError::Artifact(ArtifactError::MissingArtifact))
    );
    assert_eq!(
        s.model.permit(&candidate, Some(&completion())),
        Err(PermitError::CanonicalBlocked)
    );
    assert_eq!(s.model.last_record.get(), 21);
}

#[test]
fn v2_policy_no_ordinal_cast_reaches_publication_guard() {
    let s = at_candidate21();
    let candidate = s.stream().candidate.as_ref().unwrap();
    let wire_gate = RecordingGate::try_from(3).unwrap();
    let receipt_level = WatermarkKind::try_from(3).unwrap().achieved_gate().unwrap();
    assert_eq!(candidate.projection.gate, wire_gate);
    let mut fence = completion();
    fence.gate = receipt_level;
    assert_eq!(
        s.model.permit(candidate, Some(&fence)),
        Err(PermitError::FenceTooWeak)
    );
}
