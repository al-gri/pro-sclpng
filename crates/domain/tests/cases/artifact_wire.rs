//! Binary PSAD/PSAM/PSCO and targeted policy vectors. All evidence is synthetic.

use domain::artifact::{ArtifactError, ArtifactKind};
use domain::event::{LevelChange, MarketPayload, Side};
use domain::numeric::PriceTicks;
use domain::policy::{PolicyError, RecordingGate, WatermarkKind};
use domain::record::Record;

use crate::support::artifact_bodies::{Body, decode_body, encode_body};
use crate::support::artifact_bytes::{
    check_manifest_length, decode_descriptor, decode_manifest, encode_descriptor, encode_manifest,
};
use crate::support::binary::{Error, ErrorKind, crc32};
use crate::support::binary_fixtures::{self as frozen, golden};
use crate::support::commitment::{decode_commitment, encode_commitment};
use crate::support::fixtures;
use crate::support::wal::decode_exact;

fn repair_crc(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let checksum = crc32(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum.to_le_bytes());
}

fn config_record(bytes: &[u8]) -> domain::record::ConfigDefinition {
    match decode_exact(bytes, false, &Default::default())
        .expect("frozen ConfigDefinition frame")
        .value
    {
        Record::ConfigDefinition(value) => value,
        other => panic!("expected config record, got {other:?}"),
    }
}

fn config_body(bytes: &[u8]) -> crate::support::bodies::ConfigBody {
    match decode_body(ArtifactKind::Config, bytes).expect("frozen config body") {
        Body::Config(value) => value,
        other => panic!("expected config body, got {other:?}"),
    }
}

#[test]
fn af_markdown_wrapper_is_crlf_portable_without_changing_fixture_bytes() {
    let lf = frozen::AF_MD.replace("\r\n", "\n");
    let crlf = lf.replace('\n', "\r\n");

    for id in ["AF-N1", "AF-B1", "AF-C1", "AF-F1", "AF-V1"] {
        let (native_descriptor, native_body) = frozen::markdown_artifact_bytes(frozen::AF_MD, id);
        let (lf_descriptor, lf_body) = frozen::markdown_artifact_bytes(&lf, id);
        let (crlf_descriptor, crlf_body) = frozen::markdown_artifact_bytes(&crlf, id);
        let original = frozen::original(id);

        assert_eq!(native_descriptor, lf_descriptor, "{id} native descriptor");
        assert_eq!(native_body, lf_body, "{id} native body");
        assert_eq!(crlf_descriptor, lf_descriptor, "{id} CRLF descriptor");
        assert_eq!(crlf_body, lf_body, "{id} CRLF body");
        assert_eq!(lf_descriptor, original.descriptor, "{id} frozen descriptor");
        assert_eq!(lf_body, original.body, "{id} frozen body");
    }
}

#[test]
fn af_psad_bodies_and_psam_match_frozen_bytes_without_hashing() {
    let names = ["AF-N1", "AF-B1", "AF-C1", "AF-F1", "AF-V1", "AF-I1"];
    let artifacts: Vec<_> = names.iter().map(|name| frozen::original(name)).collect();

    for (name, artifact) in names.iter().zip(&artifacts) {
        let descriptor =
            decode_descriptor(&artifact.descriptor, fixtures::archive()).expect("frozen PSAD");
        assert_eq!(
            descriptor.metadata.body_length,
            u64::try_from(artifact.body.len()).unwrap(),
            "{name}"
        );
        assert_eq!(
            descriptor.metadata.body_sha256, artifact.body_digest,
            "{name}"
        );
        assert_eq!(
            encode_descriptor(&descriptor).unwrap(),
            artifact.descriptor,
            "{name}"
        );

        let body =
            decode_body(descriptor.metadata.identity.kind, &artifact.body).expect("frozen body");
        assert_eq!(encode_body(&body).unwrap(), artifact.body, "{name}");
        artifact
            .witness(&artifact.descriptor, &artifact.body)
            .expect("enumerated frozen pair");

        if !artifact.body.is_empty() {
            let mut changed = artifact.body.clone();
            changed[0] ^= 1;
            assert_eq!(
                artifact
                    .witness(&artifact.descriptor, &changed)
                    .unwrap_err(),
                ArtifactError::ArtifactDigestMismatch,
                "{name}"
            );
        }
    }

    let manifest = golden("PSAM-SIX");
    let entries = decode_manifest(&manifest).expect("frozen PSAM");
    assert_eq!(entries.len(), artifacts.len());
    assert_eq!(encode_manifest(&entries).unwrap(), manifest);
    for entry in &entries {
        let artifact = artifacts
            .iter()
            .find(|artifact| artifact.reference == entry.reference)
            .expect("manifest entry belongs to frozen closure");
        check_manifest_length(entry, &artifact.descriptor).unwrap();
    }

    let first = &entries[0];
    let artifact = artifacts
        .iter()
        .find(|artifact| artifact.reference == first.reference)
        .unwrap();
    let mut wrong_length = artifact.descriptor.clone();
    wrong_length.push(0);
    assert_eq!(
        check_manifest_length(first, &wrong_length),
        Err(Error::new(
            0,
            ErrorKind::Artifact(ArtifactError::ArtifactLengthMismatch)
        ))
    );
}

#[test]
fn psco_snapshot_and_update_goldens_decode_to_exact_outputs() {
    let snapshot_bytes = golden("PSCO-SNAPSHOT");
    let snapshot = decode_commitment(&snapshot_bytes).unwrap();
    assert_eq!(snapshot, vec![fixtures::snapshot()]);
    assert_eq!(encode_commitment(&snapshot).unwrap(), snapshot_bytes);

    let updates_bytes = golden("PSCO-UPDATES");
    let expected = vec![
        MarketPayload::Update(vec![
            LevelChange::Set(fixtures::level(Side::Bid, 2000, 2)),
            LevelChange::Delete {
                side: Side::Ask,
                price: PriceTicks::new(2002).unwrap(),
            },
        ]),
        MarketPayload::Update(vec![LevelChange::Set(fixtures::level(Side::Ask, 2003, 1))]),
    ];
    assert_eq!(decode_commitment(&updates_bytes).unwrap(), expected);
    assert_eq!(encode_commitment(&expected).unwrap(), updates_bytes);

    let mut invalid = updates_bytes;
    invalid[15] = 3;
    assert_eq!(
        decode_commitment(&invalid),
        Err(Error::new(
            15,
            ErrorKind::Unsupported {
                field: "PSCO.operation",
                value: 3,
            }
        ))
    );
}

#[test]
fn warmup_and_quiet_body_goldens_bind_scope_time_and_basis_records() {
    let warmup_bytes = golden("WARMUP-BODY");
    let warmup = decode_body(ArtifactKind::Warmup, &warmup_bytes).unwrap();
    match &warmup {
        Body::Warmup(value) => {
            assert_eq!(value.scope.archive, fixtures::archive());
            assert_eq!(value.scope.barrier, 9);
            assert_eq!(value.anchor, fixtures::record(10));
            assert_eq!(value.update_count, 1);
            assert_eq!(value.elapsed_ns, 7);
            assert_eq!(value.observed_at_ns, 17);
            assert_eq!(
                value.basis_records,
                vec![
                    fixtures::record(9),
                    fixtures::record(10),
                    fixtures::record(16)
                ]
            );
        }
        other => panic!("expected warmup body, got {other:?}"),
    }
    assert_eq!(encode_body(&warmup).unwrap(), warmup_bytes);

    let quiet_bytes = golden("QUIET-BODY");
    let quiet = decode_body(ArtifactKind::Freshness, &quiet_bytes).unwrap();
    match &quiet {
        Body::Freshness(value) => {
            assert_eq!(value.scope.archive, fixtures::archive());
            assert_eq!(value.scope.barrier, 9);
            assert_eq!(value.anchor, Some(fixtures::record(10)));
            assert_eq!(value.basis, fixtures::record(15));
            assert_eq!(value.freshness, domain::record::Freshness::QuietVerified);
            assert_eq!(value.observed_at_ns, 15);
            assert_eq!(value.valid_from_ns, Some(15));
            assert_eq!(value.valid_until_ns, Some(25));
            assert_eq!(
                value.basis_records,
                vec![
                    fixtures::record(9),
                    fixtures::record(10),
                    fixtures::record(15)
                ]
            );
        }
        other => panic!("expected freshness body, got {other:?}"),
    }
    assert_eq!(encode_body(&quiet).unwrap(), quiet_bytes);
}

#[test]
fn v2_policy_binary_all_tags_match_wal_and_descriptor() {
    for silence in [1_u8, 2] {
        for gate in [1_u8, 2, 3] {
            let (artifact, wal) = frozen::policy_variant(silence, gate);
            let descriptor = decode_descriptor(&artifact.descriptor, fixtures::archive()).unwrap();
            assert_eq!(descriptor.metadata.identity.kind, ArtifactKind::Config);
            assert_eq!(descriptor.metadata.body_length, 135);
            assert_eq!(descriptor.metadata.body_sha256, artifact.body_digest);
            assert_eq!(encode_descriptor(&descriptor).unwrap(), artifact.descriptor);

            let body = config_body(&artifact.body);
            let record = config_record(&wal);
            assert_eq!(record.evidence, artifact.reference);
            assert_eq!(record.fields.silence_rule.tag(), silence);
            assert_eq!(record.fields.recording_gate.tag(), gate);
            assert_eq!(body.policy.fields.silence_rule.tag(), silence);
            assert_eq!(body.policy.fields.recording_gate.tag(), gate);
            assert_eq!(artifact.body[83], silence);
            assert_eq!(artifact.body[109], gate);
            assert_eq!(wal[137], silence);
            assert_eq!(wal[163], gate);
            assert_eq!(record.fields.validate_mirror(body.policy.fields), Ok(()));
            assert_eq!(encode_body(&Body::Config(body)).unwrap(), artifact.body);
        }
    }
}

#[test]
fn v2_policy_binary_unsupported_and_supported_mismatch_reach_target_guards() {
    let (base_artifact, base_wal) = frozen::policy_variant(1, 3);
    for value in [0_u8, 255] {
        let mut wal = base_wal.clone();
        wal[137] = value;
        repair_crc(&mut wal);
        assert_eq!(
            decode_exact(&wal, false, &Default::default()),
            Err(Error::new(
                137,
                ErrorKind::Unsupported {
                    field: "Config.silence_rule",
                    value: value.into(),
                }
            ))
        );

        let mut body = base_artifact.body.clone();
        body[83] = value;
        assert_eq!(
            decode_body(ArtifactKind::Config, &body),
            Err(Error::new(
                83,
                ErrorKind::Unsupported {
                    field: "Config.silence_rule",
                    value: value.into(),
                }
            ))
        );

        let mut wal = base_wal.clone();
        wal[163] = value;
        repair_crc(&mut wal);
        assert_eq!(
            decode_exact(&wal, false, &Default::default()),
            Err(Error::new(
                163,
                ErrorKind::Unsupported {
                    field: "Config.recording_gate",
                    value: value.into(),
                }
            ))
        );

        let mut body = base_artifact.body.clone();
        body[109] = value;
        assert_eq!(
            decode_body(ArtifactKind::Config, &body),
            Err(Error::new(
                109,
                ErrorKind::Unsupported {
                    field: "Config.recording_gate",
                    value: value.into(),
                }
            ))
        );
    }

    let (_, wal_silence_two) = frozen::policy_variant(2, 3);
    let (body_silence_one, _) = frozen::policy_variant(1, 3);
    assert_eq!(
        config_record(&wal_silence_two)
            .fields
            .validate_mirror(config_body(&body_silence_one.body).policy.fields),
        Err(PolicyError::InvalidPayload {
            field: "Config.silence_rule",
            detail: "PolicyRepresentationMismatch",
        })
    );

    let (_, wal_gate_one) = frozen::policy_variant(1, 1);
    let (body_gate_three, _) = frozen::policy_variant(1, 3);
    assert_eq!(
        config_record(&wal_gate_one)
            .fields
            .validate_mirror(config_body(&body_gate_three.body).policy.fields),
        Err(PolicyError::InvalidPayload {
            field: "Config.recording_gate",
            detail: "PolicyRepresentationMismatch",
        })
    );

    assert_eq!(RecordingGate::try_from(3), Ok(RecordingGate::Durable));
    assert_eq!(WatermarkKind::try_from(3), Ok(WatermarkKind::Written));
    assert_eq!(
        WatermarkKind::Written.achieved_gate(),
        Some(RecordingGate::Written)
    );
    assert!(
        !WatermarkKind::Written
            .achieved_gate()
            .unwrap()
            .covers(RecordingGate::Durable)
    );
}
