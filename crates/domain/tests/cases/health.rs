//! R1/R3/C2 recorded-input model assertions; all source/evidence facts synthetic.

use crate::support::bodies::{EvidenceScope, FreshnessBody, VerificationBody, WarmupBody};
use crate::support::fixtures::{self, cursor, raw_id, snapshot, update};
use crate::support::health::*;
use crate::support::scenario::{Scenario, mock_node};
use domain::artifact::{ArtifactError, ArtifactKind};
use domain::event::*;
use domain::identity::*;
use domain::policy::*;
use domain::record::*;

fn codes(out: &StepResult) -> Vec<DiagnosticCode> {
    out.diagnostics.iter().map(|d| d.code.clone()).collect()
}

fn assert_effect(out: &StepResult, index: usize, raw: u64, sub: u32, applied: u64, output: u32) {
    let event = &out.effects[index];
    assert_eq!(event.source_candidate.raw, raw_id(raw));
    assert_eq!(event.source_candidate.index.get(), sub);
    assert_eq!(event.event_id.cursor, cursor(applied, output));
    assert_eq!(event.available_at, cursor(applied, output));
    assert_eq!(event.as_of.record_frontier.get(), applied);
    assert_eq!(event.source_ingest_order.get(), raw);
    assert_eq!(event.received.monotonic.ns.get(), raw);
}

#[derive(Clone, Copy, Debug)]
enum DynamicResolution {
    Missing,
    Inapplicable,
    Resolved,
}

fn make_body_scope_obsolete(scope: &mut EvidenceScope) {
    scope.context.config = ConfigVersion::new(99).unwrap();
}

fn install_dynamic_node(
    scenario: &mut Scenario,
    mut node: crate::support::artifacts::SyntheticArtifact,
    resolution: DynamicResolution,
) {
    match resolution {
        DynamicResolution::Missing => {}
        DynamicResolution::Inapplicable => {
            node.applicable = false;
            scenario.env.resolver.supplied.insert(node.reference, node);
        }
        DynamicResolution::Resolved => {
            scenario.env.resolver.supplied.insert(node.reference, node);
        }
    }
}

fn assert_artifact_blocked(
    scenario: &mut Scenario,
    control: Control,
    time: u64,
    expected: ArtifactError,
) {
    let before = scenario.model.clone();
    let bound_before = scenario.env.resolver.bound_count();
    let record = scenario.next_record();
    let expected_error = ModelError::Artifact(expected);

    assert_eq!(
        scenario.submit(Record::Control(ControlRecord {
            context: scenario.context(time),
            value: control,
        })),
        Err(expected_error.clone())
    );
    assert_eq!(scenario.model.last_record, before.last_record);
    assert_eq!(scenario.model.evaluation_ns, before.evaluation_ns);
    assert_eq!(scenario.model.streams, before.streams);
    assert_eq!(scenario.model.blocked, Some(expected_error));
    assert_eq!(scenario.env.resolver.bound_count(), bound_before);
    assert!(!scenario.env.prefix.records.contains(&RecordRef {
        archive: scenario.model.start.archive,
        record,
    }));
}

fn obsolete_verification_case(resolution: DynamicResolution) -> (Scenario, VerificationEvidence) {
    let mut scenario = Scenario::initial(fixtures::policy());
    scenario.raw(vec![snapshot()], 10).unwrap();

    let mut body = scenario.proof_body(10);
    make_body_scope_obsolete(&mut body.scope);
    let current = scenario.stream().binding.clone();
    let mut wire = scenario.put_verification(body);
    wire.stream = current.id;
    wire.tag = current.tag;
    wire.profile = current.feed_profile;

    let proof = wire.proof;
    match resolution {
        DynamicResolution::Missing => {
            scenario.env.resolver.supplied.remove(&proof);
        }
        DynamicResolution::Inapplicable => {
            scenario
                .env
                .resolver
                .supplied
                .get_mut(&proof)
                .unwrap()
                .applicable = false;
        }
        DynamicResolution::Resolved => {}
    }
    (scenario, wire)
}

fn obsolete_warmup_case(resolution: DynamicResolution) -> (Scenario, WarmupEvidence) {
    let mut scenario = Scenario::recovered();
    let mut body: WarmupBody = scenario.warmup_body(15);
    make_body_scope_obsolete(&mut body.scope);

    let logical = format!(
        "stream/{}/anchor/{}",
        body.scope.stream.get(),
        body.anchor.get()
    );
    let revision = scenario
        .env
        .resolver
        .supplied
        .values()
        .filter(|node| {
            node.metadata.identity.kind == ArtifactKind::Warmup
                && node.metadata.identity.logical.as_str() == logical
        })
        .count()
        + 1;
    let node = mock_node(
        scenario.next_artifact,
        ArtifactKind::Warmup,
        &logical,
        u32::try_from(revision).unwrap(),
        vec![
            scenario.model.config.as_ref().unwrap().0,
            scenario.stream().profile_ref,
        ],
    );
    scenario.next_artifact += 1;
    let reference = node.reference;
    let current = scenario.stream().binding.clone();
    let wire = WarmupEvidence {
        stream: current.id,
        tag: current.tag,
        anchor: body.anchor,
        update_count: body.update_count,
        elapsed_ns: body.elapsed_ns,
        proof: reference,
    };
    scenario.env.warmups.insert(reference, body);
    install_dynamic_node(&mut scenario, node, resolution);
    (scenario, wire)
}

fn obsolete_freshness_case(resolution: DynamicResolution) -> (Scenario, FreshnessEvidence) {
    let mut scenario = Scenario::recovered();
    let mut body: FreshnessBody = scenario.freshness_body(Freshness::Fresh, 12, None, None);
    make_body_scope_obsolete(&mut body.scope);

    let logical = format!(
        "stream/{}/basis/{}",
        body.scope.stream.get(),
        body.basis.get()
    );
    let revision = scenario
        .env
        .resolver
        .supplied
        .values()
        .filter(|node| {
            node.metadata.identity.kind == ArtifactKind::Freshness
                && node.metadata.identity.logical.as_str() == logical
        })
        .count()
        + 1;
    let node = mock_node(
        scenario.next_artifact,
        ArtifactKind::Freshness,
        &logical,
        u32::try_from(revision).unwrap(),
        vec![
            scenario.model.config.as_ref().unwrap().0,
            scenario.stream().profile_ref,
        ],
    );
    scenario.next_artifact += 1;
    let reference = node.reference;
    let current = scenario.stream().binding.clone();
    let wire = FreshnessEvidence {
        stream: current.id,
        tag: current.tag,
        freshness: body.freshness,
        basis: Some(body.basis),
        proof: reference,
    };
    scenario.env.freshness.insert(reference, body);
    install_dynamic_node(&mut scenario, node, resolution);
    (scenario, wire)
}

#[test]
fn v_r1_barrier_and_post_barrier_resync() {
    let mut s = Scenario::initial(fixtures::policy());
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::Gap(Reason::SourceGap)))
    );
    assert!(!s.model.usable_data(s.stream().binding.id));
    assert!(s.raw(vec![snapshot()], 10).unwrap().effects.is_empty());
    assert_eq!(s.stream().pending.len(), 1);
    assert_eq!(s.stream().anchor, None);
    s.gap(Reason::QueueOverflow, None, None, 11).unwrap();
    let out = s.proof(10, 12).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::PreBarrier]);
    assert!(out.effects.is_empty());
    assert_eq!(s.stream().barrier, 11);
    assert_eq!(s.stream().anchor, None);
    assert_eq!(s.stream().last_event_cursor, None);
    assert_eq!(s.stream().progress, 0);
    assert_eq!(s.model.recording.health, RecordingHealth::Degraded);
    s.raw(vec![snapshot()], 13).unwrap();
    let out = s.proof(13, 14).unwrap();
    assert_effect(&out, 0, 13, 0, 14, 0);
    assert_eq!(s.stream().book, Some(BookValidity::Warming));
    assert_eq!(s.stream().anchor.as_ref().unwrap().raw, raw_id(13));
    assert_eq!(s.stream().loss.window, None);
    s.raw(vec![update()], 15).unwrap();
    let out = s.proof(15, 16).unwrap();
    assert_effect(&out, 0, 15, 0, 16, 0);
    assert_eq!(s.stream().progress, 1);
    let out = s.warmup(17).unwrap();
    assert!(out.effects.is_empty());
    assert!(out.candidates_created.is_empty());
    assert_eq!(s.stream().book, Some(BookValidity::Usable));
    assert_eq!(s.stream().witness.as_ref().unwrap().body.elapsed_ns, 4);
    assert!(s.model.usable_data(s.stream().binding.id));
    assert_eq!(s.stream().candidate, None);
}

#[test]
fn v_r1_reordered_proofs_release_source_prefix_at_current_cursor() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.raw(vec![update()], 11).unwrap();
    let out = s.proof(11, 12).unwrap();
    assert!(out.effects.is_empty());
    assert!(s.stream().pending[0].proof.is_none());
    assert!(s.stream().pending[1].proof.is_some());
    let out = s.proof(10, 13).unwrap();
    assert_eq!(out.effects.len(), 2);
    assert_effect(&out, 0, 10, 0, 13, 0);
    assert_effect(&out, 1, 11, 0, 13, 1);
    assert_eq!(s.stream().last_event_cursor, Some(cursor(13, 1)));
    assert_eq!(s.stream().last_applied_raw.unwrap().get(), 11);
    assert_eq!(s.stream().last_valid_sample_ns, Some(11));
    assert_eq!(s.stream().progress, 1);
    assert!(s.stream().pending.is_empty());
    s.warmup(14).unwrap();
    assert!(s.model.usable_data(s.stream().binding.id));
}

#[test]
fn v_r1_multi_output_duplicate_and_conflicting_proof() {
    let mut p = fixtures::policy();
    p.fields.warmup_min_updates = Some(2);
    let mut s = Scenario::initial(p);
    s.raw(vec![snapshot()], 10).unwrap();
    s.proof(10, 11).unwrap();
    s.timer(12).unwrap();
    s.timer(13).unwrap();
    s.timer(14).unwrap();
    s.raw(vec![update(), update()], 15).unwrap();
    let body = s.proof_body(15);
    let out = s.proof_with(body.clone(), 16).unwrap();
    assert_eq!(out.effects.len(), 2);
    assert_effect(&out, 0, 15, 0, 16, 0);
    assert_effect(&out, 1, 15, 1, 16, 1);
    assert_eq!(s.stream().progress, 2);
    let mut duplicate = s.clone();
    let out = duplicate.proof_with(body.clone(), 17).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::AlreadyApplied]);
    assert!(out.effects.is_empty());
    assert_eq!(duplicate.stream().progress, 2);
    assert_eq!(duplicate.stream().last_valid_sample_ns, Some(15));
    assert_eq!(duplicate.stream().last_event_cursor, Some(cursor(16, 1)));
    let mut contradictory = body;
    contradictory.output_sha256 = [99; 32];
    let out = s.proof_with(contradictory, 17).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::Fault(Fault::ProofConflict)]);
    assert!(out.effects.is_empty());
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::ProofConflict))
    );
    assert_eq!(s.stream().barrier, 17);
    assert_eq!(s.stream().anchor, None);
    assert_eq!(s.stream().progress, 0);
    assert_eq!(s.stream().last_event_cursor, Some(cursor(16, 1)));
}

#[test]
fn v_r1_pending_duplicate_does_not_bypass_earlier_frame() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.raw(vec![update()], 11).unwrap();
    let body = s.proof_body(11);
    s.proof_with(body.clone(), 12).unwrap();
    let out = s.proof_with(body, 13).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::AlreadyVerified]);
    assert!(out.effects.is_empty());
    assert_eq!(s.stream().pending.len(), 2);
    assert_eq!(s.stream().anchor, None);
    assert_eq!(s.stream().last_event_cursor, None);
}

#[test]
fn v_r1_mixed_and_atomic_structural_failure() {
    let mut s = Scenario::initial(fixtures::policy());
    let out = s.raw(vec![snapshot(), update()], 10).unwrap();
    assert_eq!(
        codes(&out),
        [DiagnosticCode::Fault(Fault::MixedFrameUnsupported)]
    );
    assert!(out.effects.is_empty());
    assert_eq!(s.stream().barrier, 10);
    assert!(s.stream().pending.is_empty());
    let mut s = Scenario::recovered();
    let duplicate = MarketPayload::Update(vec![
        LevelChange::Set(fixtures::level(Side::Bid, 2000, 1)),
        LevelChange::Set(fixtures::level(Side::Bid, 2000, 2)),
    ]);
    let out = s.raw(vec![update(), duplicate], 15).unwrap();
    assert_eq!(
        codes(&out),
        [DiagnosticCode::Fault(Fault::Structural(
            EventError::DuplicateLevel
        ))]
    );
    assert!(out.effects.is_empty());
    assert_eq!(s.stream().anchor, None);
    assert_eq!(s.stream().progress, 0);
    assert_eq!(s.stream().last_event_cursor, Some(cursor(13, 0)));
}

#[test]
fn v_r1_pending_bounds_are_explicit_fail_closed_limits() {
    for field in ["frames", "raw_bytes", "outputs"] {
        let mut s = Scenario::initial(fixtures::policy());
        let raw = match field {
            "frames" => {
                s.raw(vec![snapshot()], 10).unwrap();
                s.raw(vec![update()], 11).unwrap();
                s.raw_input(vec![update()], 12, None, None)
            }
            "raw_bytes" => s.raw_input(vec![snapshot()], 10, None, Some(257)),
            _ => s.raw_input(vec![update(); 5], 10, None, None),
        };
        let record = s.next_record().get();
        let out = s.submit(Record::RawInput(raw)).unwrap();
        assert_eq!(
            codes(&out),
            [DiagnosticCode::Fault(Fault::PendingOverflow(field))]
        );
        assert!(out.effects.is_empty());
        assert_eq!(s.stream().barrier, record);
        assert!(s.stream().pending.is_empty());
        assert_eq!(s.stream().anchor, None);
    }
}

#[test]
fn v_r1_pending_deadline_before_equal_and_proof_at_expiry() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.timer(59).unwrap();
    assert_eq!(s.stream().pending[0].deadline_ns, 60);
    let mut proof_at_expiry = s.clone();
    let out = s.timer(60).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::Fault(Fault::PendingTimeout)]);
    assert_eq!(s.stream().barrier, 12);
    let out = proof_at_expiry.proof(10, 60).unwrap();
    assert_eq!(
        codes(&out),
        [
            DiagnosticCode::Fault(Fault::PendingTimeout),
            DiagnosticCode::PreBarrier
        ]
    );
    assert!(out.effects.is_empty());
    assert_eq!(proof_at_expiry.stream().anchor, None);
    let mut overflow = Scenario::initial(fixtures::policy());
    let out = overflow.raw(vec![snapshot()], u64::MAX - 10).unwrap();
    assert_eq!(
        codes(&out),
        [DiagnosticCode::Fault(Fault::PendingDeadlineOverflow)]
    );
    assert_eq!(overflow.stream().barrier, 10);
    assert!(overflow.stream().pending.is_empty());
}

#[test]
fn v_r1_down_up_and_config_change_do_not_reuse_pending_snapshot() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.transport(Transport::Down, 11).unwrap();
    s.transport(Transport::Up, 12).unwrap();
    let out = s.proof(10, 13).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::PreBarrier]);
    assert_eq!(s.model.transport_for(s.stream()), Transport::Up);
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::TransportDown))
    );
    assert_eq!(s.stream().barrier, 11);
    assert!(out.effects.is_empty());
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.config_change(2, 1, fixtures::policy(), 11).unwrap();
    let out = s.proof(10, 12).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::PreBarrier]);
    assert_eq!(s.stream().barrier, 11);
    assert_eq!(s.model.context().unwrap().config.get(), 2);
    s.raw(vec![snapshot()], 13).unwrap();
    let out = s.proof(13, 14).unwrap();
    assert_effect(&out, 0, 13, 0, 14, 0);
    assert_eq!(out.effects[0].context.config.get(), 2);
    let out = s.proof(10, 15).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::PreBarrier]);
    assert_eq!(s.stream().anchor.as_ref().unwrap().raw, raw_id(13));
}

#[test]
fn v_r1_old_sample_and_unproven_resync_membership() {
    let mut old = Scenario::initial(fixtures::policy());
    old.raw(vec![snapshot()], 8).unwrap();
    let out = old.proof(10, 11).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::Fault(Fault::ProofConflict)]);
    assert!(out.effects.is_empty());
    assert_eq!(old.stream().barrier, 11);
    let mut unproven = Scenario::initial(fixtures::policy());
    let raw = unproven.raw_input(vec![snapshot()], 10, None, None);
    unproven
        .env
        .normalizations
        .get_mut(&raw_id(10))
        .unwrap()
        .post_barrier_membership = false;
    unproven.submit(Record::RawInput(raw)).unwrap();
    let out = unproven.proof(10, 11).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::Fault(Fault::ProofConflict)]);
    assert!(out.effects.is_empty());
}

#[test]
fn v_r1_expired_later_proof_preserves_only_earlier_complete_frame() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.raw(vec![update()], 11).unwrap();
    let mut body = s.proof_body(11);
    body.valid_until_ns = Some(13);
    s.proof_with(body, 12).unwrap();
    let out = s.proof(10, 13).unwrap();
    assert_eq!(out.effects.len(), 1);
    assert_effect(&out, 0, 10, 0, 13, 0);
    assert_eq!(codes(&out), [DiagnosticCode::Fault(Fault::ProofExpired)]);
    assert_eq!(s.stream().last_event_cursor, Some(cursor(13, 0)));
    assert_eq!(s.stream().anchor, None);
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::ProofExpired))
    );
    assert_eq!(s.stream().barrier, 13);
    assert!(out.candidates_created.is_empty());
}

#[test]
fn h01_h12_snapshot_warmup_and_false_witness_guards() {
    let mut s = Scenario::initial(fixtures::policy());
    s.transport(Transport::Up, 10).unwrap();
    assert!(!s.model.usable_data(s.stream().binding.id));
    assert_eq!(s.stream().freshness, Freshness::Unknown);
    s.raw(vec![snapshot()], 11).unwrap();
    s.proof(11, 12).unwrap();
    let mut false_witness = s.warmup_body(13);
    false_witness.update_count = 100;
    let out = s.warmup_with(false_witness, 13).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::WitnessMismatch]);
    assert_eq!(s.stream().book, Some(BookValidity::Warming));
    assert_eq!(s.stream().witness, None);
    assert_eq!(s.stream().progress, 0);
}

#[test]
fn v_c2_progress_caps_per_output_and_freezes_after_usable() {
    let mut policy = fixtures::policy();
    policy.fields.warmup_min_updates = Some(2);
    let mut s = Scenario::initial(policy);
    s.raw(vec![snapshot()], 10).unwrap();
    s.proof(10, 11).unwrap();
    s.raw(vec![update()], 12).unwrap();
    s.proof(12, 13).unwrap();
    assert_eq!(s.stream().progress, 1);
    s.timer(14).unwrap();
    s.raw(vec![update(), update()], 15).unwrap();
    let out = s.proof(15, 16).unwrap();
    assert_eq!(out.effects.len(), 2);
    assert_eq!(s.stream().progress, 2);
    s.warmup(17).unwrap();
    let witness = s.stream().witness.clone();
    s.raw(vec![update()], 18).unwrap();
    s.proof(18, 19).unwrap();
    assert_eq!(s.stream().progress, 2);
    assert_eq!(s.stream().witness, witness);
    assert_eq!(s.stream().last_valid_sample_ns, Some(18));
}

#[test]
fn v_c2_max_is_a_supplied_boundary_state_not_billions_of_updates() {
    let mut policy = fixtures::policy();
    policy.fields.warmup_min_updates = Some(u32::MAX);
    let mut s = Scenario::initial(policy);
    s.raw(vec![snapshot()], 10).unwrap();
    s.proof(10, 11).unwrap();
    s.model
        .streams
        .get_mut(&StreamId::new(1).unwrap())
        .unwrap()
        .progress = u32::MAX - 1;
    s.raw(vec![update(), update()], 12).unwrap();
    s.proof(12, 13).unwrap();
    assert_eq!(s.stream().progress, u32::MAX);
    s.warmup(14).unwrap();
    s.raw(vec![update()], 15).unwrap();
    s.proof(15, 16).unwrap();
    assert_eq!(s.stream().progress, u32::MAX);
    assert_eq!(s.stream().book, Some(BookValidity::Usable));
    assert_eq!(s.stream().witness.as_ref().unwrap().record.get(), 14);
}

#[test]
fn v_r3_freshness_before_equal_after_none_and_overflow() {
    let mut policy = fixtures::policy();
    policy.fields.silence_rule = SilenceRule::StaleAfterDeadline;
    policy.fields.freshness_deadline_ns = Some(10);
    assert_eq!(
        ordinary_freshness(Some(5), 14, policy),
        (Freshness::Fresh, None)
    );
    assert_eq!(
        ordinary_freshness(Some(5), 15, policy),
        (Freshness::Stale, None)
    );
    assert_eq!(
        ordinary_freshness(Some(5), 16, policy),
        (Freshness::Stale, None)
    );
    policy.fields.silence_rule = SilenceRule::UnknownOnSilence;
    assert_eq!(
        ordinary_freshness(Some(5), 15, policy),
        (Freshness::Unknown, None)
    );
    assert_eq!(
        ordinary_freshness(Some(5), 16, policy),
        (Freshness::Unknown, None)
    );
    assert_eq!(
        ordinary_freshness(Some(u64::MAX - 5), u64::MAX, policy),
        (
            Freshness::Unknown,
            Some(DiagnosticCode::FreshnessDeadlineOverflow)
        )
    );
    policy.fields.freshness_deadline_ns = None;
    assert_eq!(
        ordinary_freshness(Some(5), 5, policy),
        (Freshness::Unknown, None)
    );
    assert_eq!(
        ordinary_freshness(Some(5), 100, policy),
        (Freshness::Unknown, None)
    );
}

#[test]
fn v_r3_late_proof_keeps_original_sample_and_exclusive_expiry() {
    let mut policy = fixtures::policy();
    policy.fields.silence_rule = SilenceRule::StaleAfterDeadline;
    policy.fields.freshness_deadline_ns = Some(10);
    let mut s = Scenario::initial(policy);
    s.raw(vec![snapshot()], 10).unwrap();
    s.timer(19).unwrap();
    let out = s.proof(10, 20).unwrap();
    assert_eq!(out.effects[0].available_at, cursor(12, 0));
    assert_eq!(out.effects[0].applied_at.ns.get(), 20);
    assert_eq!(out.effects[0].received.monotonic.ns.get(), 10);
    assert_eq!(s.stream().book, Some(BookValidity::Warming));
    assert_eq!(s.stream().last_valid_sample_ns, Some(10));
    assert_eq!(s.stream().freshness, Freshness::Stale);
}

fn quiet_scenario() -> Scenario {
    let mut policy = fixtures::policy();
    policy.fields.freshness_deadline_ns = None;
    policy.fields.allow_quiet_with_proof = true;
    policy.quiet_max_lifetime_ns = Some(10);
    let mut s = Scenario::initial(policy);
    s.raw(vec![snapshot()], 10).unwrap();
    s.proof(10, 11).unwrap();
    s.raw(vec![update()], 12).unwrap();
    s.proof(12, 13).unwrap();
    s.warmup(14).unwrap();
    s
}

#[test]
fn v_r3_quiet_original_observation_bounds_and_expiry() {
    let mut s = quiet_scenario();
    let body = s.freshness_body(Freshness::QuietVerified, 12, Some(12), Some(30));
    let out = s.freshness_with(body, 21).unwrap();
    assert!(codes(&out).is_empty());
    assert!(out.effects.is_empty());
    assert_eq!(s.stream().quiet.as_ref().unwrap().expires_ns, 22);
    assert_eq!(s.stream().freshness, Freshness::QuietVerified);
    assert!(s.model.usable_data(s.stream().binding.id));
    s.timer(22).unwrap();
    assert_eq!(s.stream().freshness, Freshness::Unknown);
    assert_eq!(s.stream().book, Some(BookValidity::Usable));
    s.timer(23).unwrap();
    assert_eq!(s.stream().freshness, Freshness::Unknown);
    let mut late = quiet_scenario();
    let body = late.freshness_body(Freshness::QuietVerified, 12, Some(12), Some(30));
    let out = late.freshness_with(body, 22).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::QuietExpired]);
    assert_eq!(late.stream().freshness, Freshness::Unknown);
}

#[test]
fn v_r3_quiet_future_is_not_automatically_activated_by_timer() {
    let mut s = quiet_scenario();
    let body = s.freshness_body(Freshness::QuietVerified, 12, Some(18), Some(30));
    let out = s.freshness_with(body.clone(), 17).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::QuietNotYetValid]);
    s.timer(18).unwrap();
    assert_eq!(s.stream().freshness, Freshness::Unknown);
    s.freshness_with(body, 18).unwrap();
    assert_eq!(s.stream().freshness, Freshness::QuietVerified);
    assert_eq!(s.stream().quiet.as_ref().unwrap().expires_ns, 22);
}

#[test]
fn v_r3_quiet_missing_bounds_denied_clock_and_false_fresh() {
    for (from, until) in [(None, Some(30)), (Some(12), None), (Some(12), Some(12))] {
        let mut s = quiet_scenario();
        let body = s.freshness_body(Freshness::QuietVerified, 12, from, until);
        let out = s.freshness_with(body, 15).unwrap();
        assert_eq!(codes(&out), [DiagnosticCode::InvalidQuietBounds]);
        assert_eq!(s.stream().freshness, Freshness::Unknown);
    }
    let mut denied = Scenario::recovered();
    let body = denied.freshness_body(Freshness::QuietVerified, 12, Some(12), Some(30));
    let out = denied.freshness_with(body, 15).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::QuietPolicyDenied]);
    assert_eq!(denied.stream().freshness, Freshness::Fresh);
    let mut clock = quiet_scenario();
    let mut body = clock.freshness_body(Freshness::QuietVerified, 12, Some(12), Some(30));
    body.scope.clock.clock = ClockId::new(2).unwrap();
    let out = clock.freshness_with(body, 15).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::IncomparableClock]);
    assert_eq!(clock.stream().freshness, Freshness::Unknown);
    let mut false_fresh = Scenario::recovered();
    let body = false_fresh.freshness_body(Freshness::Fresh, 12, None, None);
    let out = false_fresh.freshness_with(body, 112).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::FreshnessAssertionMismatch]);
    assert_eq!(false_fresh.stream().freshness, Freshness::Unknown);
}

#[test]
fn v_r3_old_quiet_proof_does_not_poison_new_scope() {
    let mut s = quiet_scenario();
    let body = s.freshness_body(Freshness::QuietVerified, 12, Some(12), Some(30));
    s.gap(Reason::SourceGap, None, None, 15).unwrap();
    s.raw(vec![snapshot()], 16).unwrap();
    s.proof(16, 17).unwrap();
    let out = s.freshness_with(body, 18).unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::PreBarrier]);
    assert_eq!(s.stream().anchor.as_ref().unwrap().raw, raw_id(16));
    assert_eq!(s.stream().book, Some(BookValidity::Warming));
    assert!(out.effects.is_empty());
}

#[test]
fn qa_p2_verification_resolution_precedes_body_obsolete_scope() {
    for (resolution, expected) in [
        (DynamicResolution::Missing, ArtifactError::MissingArtifact),
        (
            DynamicResolution::Inapplicable,
            ArtifactError::ArtifactUnverified,
        ),
    ] {
        let (mut scenario, wire) = obsolete_verification_case(resolution);
        assert_artifact_blocked(&mut scenario, Control::Verification(wire), 11, expected);
    }

    let (mut scenario, wire) = obsolete_verification_case(DynamicResolution::Resolved);
    let pending_before = scenario.stream().pending.clone();
    let record = scenario.next_record();
    let out = scenario
        .submit(Record::Control(ControlRecord {
            context: scenario.context(11),
            value: Control::Verification(wire),
        }))
        .unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::ObsoleteScope]);
    assert!(out.effects.is_empty());
    assert_eq!(scenario.stream().pending, pending_before);
    assert_eq!(scenario.model.last_record, record);
    assert!(scenario.env.prefix.records.contains(&RecordRef {
        archive: scenario.model.start.archive,
        record,
    }));
}

#[test]
fn qa_p2_warmup_resolution_precedes_body_obsolete_scope() {
    for (resolution, expected) in [
        (DynamicResolution::Missing, ArtifactError::MissingArtifact),
        (
            DynamicResolution::Inapplicable,
            ArtifactError::ArtifactUnverified,
        ),
    ] {
        let (mut scenario, wire) = obsolete_warmup_case(resolution);
        assert_artifact_blocked(&mut scenario, Control::Warmup(wire), 15, expected);
    }

    let (mut scenario, wire) = obsolete_warmup_case(DynamicResolution::Resolved);
    let witness_before = scenario.stream().witness.clone();
    let anchor_before = scenario.stream().anchor.clone();
    let progress_before = scenario.stream().progress;
    let out = scenario
        .submit(Record::Control(ControlRecord {
            context: scenario.context(15),
            value: Control::Warmup(wire),
        }))
        .unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::ObsoleteScope]);
    assert!(out.effects.is_empty());
    assert_eq!(scenario.stream().witness, witness_before);
    assert_eq!(scenario.stream().anchor, anchor_before);
    assert_eq!(scenario.stream().progress, progress_before);
}

#[test]
fn qa_p2_freshness_resolution_precedes_body_obsolete_scope() {
    for (resolution, expected) in [
        (DynamicResolution::Missing, ArtifactError::MissingArtifact),
        (
            DynamicResolution::Inapplicable,
            ArtifactError::ArtifactUnverified,
        ),
    ] {
        let (mut scenario, wire) = obsolete_freshness_case(resolution);
        assert_artifact_blocked(&mut scenario, Control::Freshness(wire), 15, expected);
    }

    let (mut scenario, wire) = obsolete_freshness_case(DynamicResolution::Resolved);
    let freshness_before = scenario.stream().freshness;
    let quiet_before = scenario.stream().quiet.clone();
    let anchor_before = scenario.stream().anchor.clone();
    let out = scenario
        .submit(Record::Control(ControlRecord {
            context: scenario.context(15),
            value: Control::Freshness(wire),
        }))
        .unwrap();
    assert_eq!(codes(&out), [DiagnosticCode::ObsoleteScope]);
    assert!(out.effects.is_empty());
    assert_eq!(scenario.stream().freshness, freshness_before);
    assert_eq!(scenario.stream().quiet, quiet_before);
    assert_eq!(scenario.stream().anchor, anchor_before);
}

#[test]
fn h19_missing_current_artifact_blocks_without_advancing_state() {
    let mut s = Scenario::recovered();
    let old = s.model.clone();
    let config_ref = s.model.config.as_ref().unwrap().0;
    s.env.resolver.supplied.remove(&config_ref);
    assert_eq!(
        s.timer(15),
        Err(ModelError::Artifact(ArtifactError::MissingArtifact))
    );
    assert_eq!(s.model.last_record, old.last_record);
    assert_eq!(s.model.evaluation_ns, old.evaluation_ns);
    assert_eq!(s.model.streams, old.streams);
    assert_eq!(
        s.model.blocked,
        Some(ModelError::Artifact(ArtifactError::MissingArtifact))
    );
    assert!(!s.model.usable_data(s.stream().binding.id));
}

#[test]
fn proof_forward_basis_is_rejected_before_scope_transition() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    let mut body: VerificationBody = s.proof_body(10);
    body.basis_records.push(RecordNo::new(11).unwrap());
    let old = s.model.clone();
    assert_eq!(
        s.proof_with(body, 11),
        Err(ModelError::Event(EventError::FutureCausalReference))
    );
    assert_eq!(s.model.last_record, old.last_record);
    assert_eq!(s.model.streams, old.streams);
}

#[test]
fn h13_spec_activation_invalidates_only_after_declared_new_spec() {
    let mut s = Scenario::recovered();
    let binding = s.stream().binding.clone();
    assert!(s.model.usable_data(binding.id));

    let mut fields = crate::support::scenario::numeric_spec().fields().clone();
    fields.reference.version = SpecVersion::new(2).unwrap();
    fields.price_increment = "0.10".parse().unwrap();
    let numeric = domain::qualified::NumericSpec::new(fields).unwrap();
    let node = crate::support::scenario::mock_node(
        900,
        domain::artifact::ArtifactKind::InstrumentSpec,
        "instrument/1",
        2,
        vec![],
    );
    s.env
        .instruments
        .insert(node.reference, (binding.instrument_slot, numeric.clone()));
    s.env.resolver.supplied.insert(node.reference, node.clone());

    s.submit(Record::InstrumentSpec(InstrumentSpecRecord {
        context: s.context(15),
        slot: binding.instrument_slot,
        numeric,
        provenance: node.reference,
    }))
    .unwrap();
    let definition_record = s.model.last_record;
    assert!(s.model.usable_data(binding.id));

    let activation_record = s.next_record();
    s.submit(Record::Control(ControlRecord {
        context: s.context(16),
        value: Control::SpecActivate {
            slot: binding.instrument_slot,
            expected: SpecVersion::new(1).unwrap(),
            next: SpecVersion::new(2).unwrap(),
        },
    }))
    .unwrap();
    let state = s.stream();
    assert_eq!(definition_record.get() + 1, activation_record.get());
    assert_eq!(state.binding.tag.spec, SpecVersion::new(2).unwrap());
    assert_eq!(state.binding.spec.version, SpecVersion::new(2).unwrap());
    assert_eq!(state.barrier, activation_record.get());
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(Fault::ContextChanged))
    );
    assert_eq!(state.freshness, Freshness::Unknown);
    assert_eq!(state.anchor, None);
    assert_eq!(state.witness, None);
    assert!(!s.model.usable_data(binding.id));
}

#[test]
fn h16_recording_health_recovery_does_not_resync_invalid_book() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![snapshot()], 10).unwrap();
    s.gap(Reason::QueueOverflow, None, None, 11).unwrap();
    assert_eq!(s.model.recording.health, RecordingHealth::Degraded);
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::Gap(Reason::QueueOverflow)))
    );

    s.receipt(
        RecordingHealth::Healthy,
        WatermarkKind::Durable,
        Some(11),
        12,
    )
    .unwrap();
    assert_eq!(s.model.recording.health, RecordingHealth::Healthy);
    assert_eq!(
        s.stream().book,
        Some(BookValidity::Invalid(Fault::Gap(Reason::QueueOverflow)))
    );
    assert_eq!(s.stream().anchor, None);
    assert!(!s.model.usable_data(s.stream().binding.id));
}

#[test]
fn h18_restart_has_new_archive_clock_and_no_inherited_ready_state() {
    let old = Scenario::recovered();
    assert!(old.model.usable_data(old.stream().binding.id));
    assert!(old.stream().candidate.is_some());

    let mut env = crate::support::model_env::ModelEnv::default();
    let start = ArchiveStart {
        archive: ArchiveId::new([3; 16]).unwrap(),
        session: CaptureSessionId::new([4; 16]).unwrap(),
        clock: ClockId::new(2).unwrap(),
        mode: old.model.start.mode,
        previous_archive: Some(old.model.start.archive),
    };
    let restarted = HealthModel::new(start.clone(), &mut env).unwrap();
    assert_eq!(restarted.start, start);
    assert_eq!(restarted.last_record, RecordNo::new(1).unwrap());
    assert_eq!(restarted.evaluation_ns, 0);
    assert!(restarted.timeline.active().is_none());
    assert!(restarted.config.is_none());
    assert!(restarted.active_specs.is_empty());
    assert!(restarted.streams.is_empty());
    assert!(restarted.transport.is_empty());
    assert_eq!(restarted.recording.health, RecordingHealth::Unknown);
    assert_eq!(restarted.recording.last_receipt, None);
    assert!(restarted.blocked.is_none());
}

#[test]
fn v_r4_profile_norm_blocks_dependent_input_without_guessing_support() {
    let mut s = Scenario::initial(fixtures::policy());
    s.config_change(2, 2, fixtures::policy(), 10).unwrap();
    let after_activation = s.model.clone();
    let stream = s.stream().clone();
    assert_eq!(stream.binding.feed_profile.get(), 1);
    assert_eq!(s.model.context().unwrap().normalizer.get(), 2);
    assert_eq!(
        stream.book,
        Some(BookValidity::Invalid(Fault::ContextChanged))
    );
    assert_eq!(stream.last_event_cursor, None);

    assert_eq!(
        s.timer(11),
        Err(ModelError::Artifact(
            ArtifactError::UnsupportedNormalizerBinding
        ))
    );
    assert_eq!(s.model.last_record, after_activation.last_record);
    assert_eq!(s.model.evaluation_ns, after_activation.evaluation_ns);
    assert_eq!(s.model.streams, after_activation.streams);
    assert_eq!(
        s.model.blocked,
        Some(ModelError::Artifact(
            ArtifactError::UnsupportedNormalizerBinding
        ))
    );
}
