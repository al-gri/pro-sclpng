//! Frozen, independently specified synthetic byte witnesses. This is a finite
//! fixture dictionary, NOT a SHA algorithm or production artifact loader.

use domain::artifact::{ArtifactError, ArtifactKind, ArtifactRef};
use domain::identity::*;

use super::artifact_bodies::{Body, decode_body};
use super::artifact_bytes::decode_descriptor;
use super::artifacts::{SyntheticArtifact, SyntheticBytes};
use super::binary::literal_hex;
use super::bodies::SyntheticNormalization;
use super::fixtures;
use super::model_env::ModelEnv;

pub const AF_MD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/domain/artifacts-v1.md"
));
pub const FROZEN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/domain/wal-frames-v1.txt"
));
pub const POLICIES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/domain/policy-variants-v1.txt"
));

pub const SINGLE: &[&str] = &[
    "W01",
    "SPEC2",
    "STREAM3",
    "CONFIG4",
    "UP5",
    "RAW6",
    "GAP7",
    "RAW8",
    "TIMER9",
    "SEAL10",
    "ARCHIVE11",
];
pub const MULTI0: &[&str] = &[
    "W01",
    "SPEC2",
    "STREAM3",
    "CONFIG4",
    "UP5",
    "RAW6",
    "GAP7",
    "MULTI-SEAL8",
];
pub const MULTI1: &[&str] = &[
    "MULTI-START9",
    "RAW10",
    "TIMER11",
    "MULTI-SEAL12",
    "MULTI-ARCHIVE13",
];
pub const OPEN: &[&str] = &[
    "W01",
    "SPEC2",
    "STREAM3",
    "CONFIG4",
    "UP5",
    "RAW6",
    "OPEN-GAP7",
    "OPEN-SEAL8",
    "OPEN-ARCHIVE9",
];

pub fn golden(id: &str) -> Vec<u8> {
    let hex = FROZEN
        .lines()
        .find_map(|line| {
            let (name, hex) = line.split_once(' ')?;
            (name == id).then_some(hex)
        })
        .expect("named frozen fixture");
    literal_hex(hex)
}

/// Concatenation of literal frames only: no length/tag/CRC/seal is computed.
pub fn sequence(ids: &[&str]) -> Vec<u8> {
    ids.iter().flat_map(|id| golden(id)).collect()
}

fn digest(hex: &str) -> [u8; 32] {
    literal_hex(hex).try_into().expect("frozen 32-byte digest")
}

#[derive(Clone, Debug)]
pub struct FrozenArtifact {
    pub reference: ArtifactRef,
    pub descriptor: Vec<u8>,
    pub body: Vec<u8>,
    pub body_digest: [u8; 32],
}

impl FrozenArtifact {
    /// A byte-equality witness over an explicitly enumerated frozen pair. An
    /// arbitrary well-formed ref cannot enter this dictionary or become verified.
    pub fn witness(&self, descriptor: &[u8], body: &[u8]) -> Result<SyntheticBytes, ArtifactError> {
        if body.len() != self.body.len() {
            return Err(ArtifactError::ArtifactLengthMismatch);
        }
        if descriptor != self.descriptor || body != self.body {
            return Err(ArtifactError::ArtifactDigestMismatch);
        }
        Ok(SyntheticBytes {
            descriptor_digest: *self.reference.claimed_digest(),
            body_digest: self.body_digest,
            body_length: u64::try_from(body.len()).expect("bounded fixture"),
        })
    }

    pub fn install_synthetic(&self, env: &mut ModelEnv, applicable: bool) {
        let descriptor = decode_descriptor(&self.descriptor, fixtures::archive()).unwrap();
        let body = decode_body(descriptor.metadata.identity.kind, &self.body).unwrap();
        let required = match &body {
            Body::Opaque(_) | Body::InstrumentSpec(_, _) => vec![],
            Body::Config(v) => vec![v.normalizer_ref],
            Body::FeedProfile(v) => {
                let mut refs = v.supported_normalizers.clone();
                refs.push(v.basis);
                refs
            }
            // These frozen evidence fixtures name config1/profile1 explicitly;
            // this is not a generic choice of a latest or default registry.
            Body::Verification(v) => vec![
                original("AF-C1").reference,
                original("AF-F1").reference,
                v.continuity_basis,
            ],
            Body::Warmup(_) | Body::Freshness(_) => {
                vec![original("AF-C1").reference, original("AF-F1").reference]
            }
        };
        descriptor
            .metadata
            .validate_dependencies(&required, &[])
            .unwrap();
        assert_eq!(descriptor.metadata.body_sha256, self.body_digest);
        let observed = self.witness(&self.descriptor, &self.body).unwrap();
        env.resolver.supplied.insert(
            self.reference,
            SyntheticArtifact {
                reference: self.reference,
                metadata: descriptor.metadata,
                descriptor_length: u64::try_from(self.descriptor.len()).unwrap(),
                observed: Some(observed),
                required_dependencies: required,
                optional_basis: vec![],
                applicable,
            },
        );
        match body {
            Body::Opaque(_) => {}
            Body::Config(v) => {
                env.configs.insert(self.reference, v);
            }
            Body::FeedProfile(v) => {
                env.profiles.insert(self.reference, v);
            }
            Body::InstrumentSpec(slot, v) => {
                env.instruments.insert(self.reference, (slot, v));
            }
            Body::Verification(v) => {
                env.verifications.insert(self.reference, v);
            }
            Body::Warmup(v) => {
                env.warmups.insert(self.reference, v);
            }
            Body::Freshness(v) => {
                env.freshness.insert(self.reference, v);
            }
        }
    }
}

pub(crate) fn markdown_artifact_bytes(markdown: &str, id: &str) -> (Vec<u8>, Vec<u8>) {
    // Git may materialize this Markdown fixture with CRLF on Windows. Normalize
    // only the textual wrapper used to locate headings/fences; literal hex is
    // decoded afterwards, so descriptor/body bytes remain platform-invariant.
    let normalized = markdown.replace("\r\n", "\n");
    let header = format!("\n## {id}\n");
    let section = normalized.split_once(&header).expect("AF heading").1;
    let descriptor = section.split_once("Descriptor:\n```text\n").unwrap().1;
    let body = section.split_once("\nBody:\n```text\n").unwrap().1;
    (
        literal_hex(descriptor.split_once("```").unwrap().0),
        literal_hex(body.split_once("```").unwrap().0),
    )
}

pub fn original(id: &str) -> FrozenArtifact {
    let (reference, body_digest) = match id {
        "AF-N1" => (
            "4fb4d8059243f24a6f7fbd33b8773bce4ea520c19f01b2074404b2c6ab5e7ded",
            "031e12d6c823b0690c8f20f6354bb6f6e7b5852116759efdbe57bf1c72643a8d",
        ),
        "AF-B1" => (
            "ede784f697e400d548203d6322bb68c0029c22246f2839624bc3838d09803e80",
            "7b7b0c0cfb108323df644a6d17d4227824769756b579f5bc16eba2440186bcfa",
        ),
        "AF-C1" => (
            "c3e9f77e412956d8e5e6c95e734140150e464d17b7c5f251dbaca0658390cef0",
            "d3827df81929780ebd89539b091c14a2873f9934743947c080d01a7211544546",
        ),
        "AF-F1" => (
            "eb68b12f25f22563a84362680147f58b0b8c123d533672e1e9f3a5ddff9cfbbd",
            "e353efcf0522137235c3e202acee70a4f1ad8b79767bbd568757aef3e2cd2f06",
        ),
        "AF-V1" => (
            "f96bff366da5fc98965b81a3bd55c24a99969ce6e426d272ecd878b8ad9605de",
            "13b3cb6cbd76450a7199804c90487d7b91aa4ec386b55f3e8327a7a54738bde0",
        ),
        "AF-I1" => (
            "732ec3f3edc5f3a381771935fc0b381ecbc77c1bfc69204d201d7535addca26e",
            "b05d0369da1780cc5d416f55ed456495f6656414f4cbee6dc40f4d724d529dde",
        ),
        _ => panic!("unknown frozen artifact"),
    };
    let (descriptor, body) = if id == "AF-I1" {
        (golden("AF-I1-DESC"), golden("AF-I1-BODY"))
    } else {
        markdown_artifact_bytes(AF_MD, id)
    };
    FrozenArtifact {
        reference: format!("sha256:{reference}").parse().unwrap(),
        descriptor,
        body,
        body_digest: digest(body_digest),
    }
}

pub fn policy_variant(silence: u8, gate: u8) -> (FrozenArtifact, Vec<u8>) {
    let id = format!("POLICY-{silence}-{gate}");
    let fields: Vec<_> = POLICIES
        .lines()
        .find(|line| line.starts_with(&format!("{id}|")))
        .expect("frozen policy combination")
        .split('|')
        .collect();
    assert_eq!(fields.len(), 6);
    (
        FrozenArtifact {
            reference: fields[1].parse().unwrap(),
            body_digest: digest(fields[2]),
            descriptor: literal_hex(fields[3]),
            body: literal_hex(fields[4]),
        },
        literal_hex(fields[5]),
    )
}

pub fn environment() -> ModelEnv {
    let mut env = ModelEnv::default();
    for name in ["AF-N1", "AF-B1", "AF-I1", "AF-C1", "AF-F1", "AF-V1"] {
        original(name).install_synthetic(&mut env, true);
    }
    for n in [6, 8, 10] {
        let bytes = format!("snapshot-{n}").into_bytes();
        let mut frame = fixtures::frame(
            n,
            vec![fixtures::snapshot()],
            fixtures::binding(1, Channel::BookNormal),
        );
        frame.raw_byte_len = u32::try_from(bytes.len()).unwrap();
        env.normalizations.insert(
            fixtures::raw_id(n),
            SyntheticNormalization {
                frame,
                output_sha256: digest(
                    "3a5c48fcddc983224bbfd8907540b2beee16fb527f980d8e8e9539ed473323ea",
                ),
                post_barrier_membership: true,
            },
        );
    }
    env
}

pub fn instrument() -> (InstrumentSlot, domain::qualified::NumericSpec) {
    let fixture = original("AF-I1");
    match decode_body(ArtifactKind::InstrumentSpec, &fixture.body).unwrap() {
        Body::InstrumentSpec(slot, spec) => (slot, spec),
        _ => unreachable!("frozen kind"),
    }
}
