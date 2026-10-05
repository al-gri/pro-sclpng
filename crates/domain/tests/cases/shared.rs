//! R6 shared transport with an independently recovered third stream.
//! All setup uses recorded model inputs, not direct mutation of ready state.

use crate::support::bodies::*;
use crate::support::fixtures::{self, snapshot, update};
use crate::support::health::*;
use crate::support::scenario::{Scenario, mock_node, numeric_spec, reference};
use domain::artifact::ArtifactKind;
use domain::event::*;
use domain::identity::*;
use domain::qualified::NumericSpec;
use domain::record::*;

fn add_profile(s: &mut Scenario, binding: &StreamBinding, id: u64) {
    let normalizer_ref = s.model.config.as_ref().unwrap().1.normalizer_ref;
    let basis = reference(5);
    let node = mock_node(
        id,
        ArtifactKind::FeedProfile,
        &format!("stream/{}", binding.id.get()),
        1,
        vec![normalizer_ref, basis],
    );
    s.env.profiles.insert(
        node.reference,
        FeedProfileBody {
            stream: binding.id,
            instrument: binding.spec.instrument.clone(),
            channel: binding.channel,
            version: binding.feed_profile,
            supported_normalizers: vec![normalizer_ref],
            basis,
        },
    );
    s.env.resolver.supplied.insert(node.reference, node);
}

fn apply_frame(
    s: &mut Scenario,
    binding: &StreamBinding,
    payload: MarketPayload,
    artifact: u64,
) -> StepResult {
    let raw_record = s.next_record();
    let id = RawFrameId {
        archive: fixtures::archive(),
        record: raw_record,
    };
    let kind = if matches!(payload, MarketPayload::Snapshot(_)) {
        BookEvidenceKind::Snapshot
    } else {
        BookEvidenceKind::Delta
    };
    let raw = RawInput {
        context: s.context(14),
        stream: binding.id,
        tag: binding.tag,
        attempt: CaptureAttemptNo::new(
            s.model.streams[&binding.id].loss.accounted_frontier + 1,
        )
        .unwrap(),
        bytes: vec![b's'],
    };
    let frame = NormalizedFrame {
        source: SourceMetadata {
            raw: id,
            binding: binding.clone(),
            context: s.model.context().unwrap(),
            received: ReceiveSample {
                unix_ns: raw.context.unix_ns,
                monotonic: MonotonicSample {
                    scope: fixtures::clock(),
                    ns: MonotonicNs::new(14),
                },
            },
        },
        raw_byte_len: 1,
        outputs: vec![NormalizedOutput {
            payload,
            timestamp: SourceTimestamp::Unknown,
        }],
    };
    s.env.normalizations.insert(
        id,
        SyntheticNormalization {
            frame,
            output_sha256: [8; 32],
            post_barrier_membership: true,
        },
    );
    assert!(s.submit(Record::RawInput(raw)).unwrap().effects.is_empty());
    let state = &s.model.streams[&binding.id];
    let body = VerificationBody {
        scope: s.model.scope(state).unwrap(),
        raw: raw_record,
        kind,
        raw_sample_ns: 14,
        not_before_ns: state.barrier_evaluation_ns,
        valid_until_ns: None,
        output_count: 1,
        output_sha256: [8; 32],
        continuity_basis: reference(5),
        basis_records: (1..s.next_record().get()).map(fixtures::record).collect(),
    };
    let node = mock_node(
        artifact,
        ArtifactKind::Verification,
        &format!("stream/{}/raw/{}", binding.id.get(), raw_record.get()),
        1,
        vec![s.model.config.as_ref().unwrap().0, state.profile_ref, reference(5)],
    );
    let wire = VerificationEvidence {
        stream: binding.id,
        tag: binding.tag,
        raw: raw_record,
        kind,
        profile: binding.feed_profile,
        proof: node.reference,
    };
    s.env.verifications.insert(node.reference, body);
    s.env.resolver.supplied.insert(node.reference, node);
    s.submit(Record::Control(ControlRecord {
        context: s.context(14),
        value: Control::Verification(wire),
    }))
    .unwrap()
}

#[test]
fn v_r6_shared_registration_down_and_epoch_preserve_other_connection() {
    let mut s = Scenario::recovered();
    let mut numeric = numeric_spec().fields().clone();
    numeric.reference.instrument.native_symbol = Token::new("XYZUSD").unwrap();
    let numeric = NumericSpec::new(numeric).unwrap();
    let spec_node = mock_node(201, ArtifactKind::InstrumentSpec, "instrument/2", 1, vec![]);
    let slot = InstrumentSlot::new(2).unwrap();
    s.env.instruments.insert(spec_node.reference, (slot, numeric.clone()));
    s.env.resolver.supplied.insert(spec_node.reference, spec_node.clone());
    s.submit(Record::InstrumentSpec(InstrumentSpecRecord {
        context: s.context(14),
        slot,
        numeric: numeric.clone(),
        provenance: spec_node.reference,
    }))
    .unwrap();
    let mut c = fixtures::binding(3, Channel::BookNormal);
    c.instrument_slot = slot;
    c.spec = numeric.fields().reference.clone();
    c.connection_id = ConnectionId::new(2).unwrap();
    add_profile(&mut s, &c, 202);
    s.submit(Record::StreamDefinition(StreamDefinition {
        context: s.context(14),
        binding: c.clone(),
        provenance: reference(202),
    }))
    .unwrap();
    s.submit(Record::Control(ControlRecord {
        context: s.context(14),
        value: Control::Transport {
            connection: c.connection_id,
            epoch: c.tag.connection,
            value: Transport::Up,
        },
    }))
    .unwrap();
    let snapshot_effect = apply_frame(&mut s, &c, snapshot(), 203);
    assert_eq!(snapshot_effect.effects.len(), 1);
    assert_eq!(snapshot_effect.effects[0].source_candidate.raw.record.get(), 18);
    assert_eq!(snapshot_effect.effects[0].available_at, fixtures::cursor(19, 0));
    let update_effect = apply_frame(&mut s, &c, update(), 204);
    assert_eq!(update_effect.effects[0].available_at, fixtures::cursor(21, 0));
    let current = &s.model.streams[&c.id];
    let body = WarmupBody {
        scope: s.model.scope(current).unwrap(),
        anchor: current.anchor.as_ref().unwrap().raw.record,
        update_count: 1,
        elapsed_ns: 0,
        observed_at_ns: 14,
        basis_records: (1..s.next_record().get()).map(fixtures::record).collect(),
    };
    let warmup = mock_node(
        205,
        ArtifactKind::Warmup,
        "stream/3/anchor/18",
        1,
        vec![s.model.config.as_ref().unwrap().0, current.profile_ref],
    );
    s.env.warmups.insert(warmup.reference, body.clone());
    s.env.resolver.supplied.insert(warmup.reference, warmup.clone());
    s.submit(Record::Control(ControlRecord {
        context: s.context(14),
        value: Control::Warmup(WarmupEvidence {
            stream: c.id,
            tag: c.tag,
            anchor: body.anchor,
            update_count: 1,
            elapsed_ns: 0,
            proof: warmup.reference,
        }),
    }))
    .unwrap();
    assert!(s.model.usable_data(c.id));
    assert!(s.model.usable_data(s.stream().binding.id));
    let a_before = s.stream().clone();
    let c_before = s.model.streams[&c.id].clone();
    let b = fixtures::binding(2, Channel::BookRpi);
    add_profile(&mut s, &b, 206);
    let result = s
        .submit(Record::StreamDefinition(StreamDefinition {
            context: s.context(14),
            binding: b.clone(),
            provenance: reference(206),
        }))
        .unwrap();
    assert_eq!(s.model.last_record.get(), 23);
    assert!(result.effects.is_empty());
    assert!(result.candidates_created.is_empty());
    assert_eq!(s.stream(), &a_before);
    assert_eq!(s.model.streams[&c.id], c_before);
    assert_eq!(s.model.transport_for(&s.model.streams[&b.id]), Transport::Up);
    assert_eq!(s.model.streams[&b.id].book, Some(BookValidity::NoSnapshot));
    assert_eq!(s.model.streams[&b.id].freshness, Freshness::Unknown);
    assert_eq!(s.model.streams[&b.id].barrier, 23);
    assert_eq!(s.model.streams[&b.id].anchor, None);
    assert!(!s.model.usable_data(b.id));
    s.transport(Transport::Down, 14).unwrap();
    assert_eq!(s.model.last_record.get(), 24);
    for id in [a_before.binding.id, b.id] {
        let state = &s.model.streams[&id];
        assert_eq!(s.model.transport_for(state), Transport::Down);
        assert_eq!(state.book, Some(BookValidity::Invalid(Fault::TransportDown)));
        assert_eq!(state.barrier, 24);
        assert_eq!(state.anchor, None);
    }
    assert_eq!(s.model.streams[&c.id], c_before);
    assert!(s.model.usable_data(c.id));
    s.epoch_connection(14).unwrap();
    assert_eq!(s.model.last_record.get(), 25);
    for id in [a_before.binding.id, b.id] {
        let state = &s.model.streams[&id];
        assert_eq!(s.model.transport_for(state), Transport::Unknown);
        assert_eq!(state.binding.tag.connection.get(), 2);
        assert_eq!(state.binding.tag.subscription.get(), 1);
        assert_eq!(state.binding.tag.book.unwrap().get(), 1);
        assert_eq!(state.book, Some(BookValidity::NoSnapshot));
        assert_eq!(state.barrier, 25);
    }
    assert_eq!(s.model.streams[&c.id], c_before);
    assert!(s.model.usable_data(c.id));
    let out = s.proof(10, 14).unwrap();
    assert_eq!(out.diagnostics[0].code, DiagnosticCode::PreBarrier);
    assert!(out.effects.is_empty());
    assert_eq!(s.model.streams[&c.id], c_before);
}
