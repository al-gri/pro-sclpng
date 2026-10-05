//! A3/R4 synthetic resolution vectors. Digest observations below are explicit
//! mock facts, not a production verifier, cryptographic test or real feed proof.

use crate::support::artifacts::*;
use crate::support::fixtures::*;
use domain::artifact::*;
use domain::event::ContextTimeline;
use domain::identity::Token;

fn reference(index: usize) -> ArtifactRef {
    format!("sha256:{index:064x}").parse().unwrap()
}

fn node(index: usize, kind: ArtifactKind, logical: &str) -> SyntheticArtifact {
    let reference = reference(index);
    SyntheticArtifact {
        reference,
        metadata: ArtifactMetadata {
            identity: ArtifactIdentity {
                archive: archive(), kind, logical: Token::new(logical).unwrap(), revision: 1,
            },
            format_version: 1,
            body_schema: 1,
            body_length: 3,
            body_sha256: [42; 32],
            dependencies: vec![],
        },
        descriptor_length: 86,
        observed: Some(SyntheticBytes {
            descriptor_digest: *reference.claimed_digest(), body_digest: [42; 32], body_length: 3,
        }),
        required_dependencies: vec![],
        optional_basis: vec![],
        applicable: true,
    }
}

fn resolver(nodes: Vec<SyntheticArtifact>) -> SyntheticResolver {
    let mut result = SyntheticResolver::default();
    for value in nodes {
        result.supplied.insert(value.reference, value);
    }
    result
}

#[test]
fn v_r4_grammar_is_exact_lowercase_without_normalization() {
    let text = format!("sha256:{}", "ab".repeat(32));
    let parsed: ArtifactRef = text.parse().unwrap();
    assert_eq!(parsed.to_string(), text);
    assert_eq!(parsed.claimed_digest(), &[0xab; 32]);
    let bad = [
        format!("sha256:{}", "A".repeat(64)),
        format!("sha256:{}", "a".repeat(63)),
        format!("sha256:{}", "a".repeat(65)),
        format!("sha256:{}", "g".repeat(64)),
        format!("sha256:{} ", "a".repeat(64)),
        format!("sha512:{}", "a".repeat(64)),
    ];
    for text in bad {
        assert_eq!(text.parse::<ArtifactRef>(), Err(ArtifactError::InvalidArtifactRef));
    }
}

#[test]
fn v_r4_parse_only_and_unsubstantiated_bytes_do_not_become_verified() {
    let mut value = node(1, ArtifactKind::Normalizer, "normalizer");
    value.observed = None;
    let mut r = resolver(vec![value]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactUnverified));
    assert_eq!(r.bound_count(), 0);
    let value = r.supplied.get_mut(&reference(1)).unwrap();
    value.observed = Some(SyntheticBytes {
        descriptor_digest: *reference(1).claimed_digest(), body_digest: [42; 32], body_length: 3,
    });
    value.applicable = false;
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactUnverified));
    assert_eq!(r.bound_count(), 0);
}

#[test]
fn v_r4_missing_dependency_has_no_partial_success() {
    let mut config = node(2, ArtifactKind::Config, "config");
    config.metadata.dependencies = vec![reference(1)];
    config.required_dependencies = vec![reference(1)];
    let mut r = resolver(vec![config]);
    assert_eq!(r.resolve(reference(2), ArtifactKind::Config, archive()), Err(ArtifactError::MissingArtifact));
    assert_eq!(r.bound_count(), 0);
    r.supplied.insert(reference(1), node(1, ArtifactKind::Normalizer, "normalizer"));
    let applied = r.resolve(reference(2), ArtifactKind::Config, archive()).unwrap();
    assert_eq!(applied.reference(), reference(2));
    assert_eq!(applied.kind(), ArtifactKind::Config);
    assert_eq!(applied.archive(), archive());
    assert_eq!(r.bound_count(), 2);
}

#[test]
fn v_r4_same_norm_reuse_early_loading_and_rebinding() {
    let norm = node(1, ArtifactKind::Normalizer, "normalizer");
    let mut r = resolver(vec![norm]);
    let before = ContextTimeline::default();
    let timeline = before.clone();
    r.resolve(reference(1), ArtifactKind::Normalizer, archive()).unwrap();
    r.resolve(reference(1), ArtifactKind::Normalizer, archive()).unwrap();
    assert_eq!(r.bound_count(), 1);
    assert_eq!(timeline, before);
    assert_eq!(timeline.active(), None);
    let mut config = node(3, ArtifactKind::Config, "config");
    config.metadata.identity.revision = 2;
    config.required_dependencies = vec![reference(1)];
    config.metadata.dependencies = vec![reference(1)];
    r.supplied.insert(reference(3), config);
    assert_eq!(r.resolve(reference(3), ArtifactKind::Config, archive()).unwrap().reference(), reference(3));
    assert_eq!(r.bound_count(), 2);
    r.supplied.insert(reference(2), node(2, ArtifactKind::Normalizer, "normalizer"));
    assert_eq!(r.resolve(reference(2), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactIdentityConflict));
    assert_eq!(r.bound_count(), 2);
}

#[test]
fn v_r4_digest_and_length_mismatch_priority() {
    let base = node(1, ArtifactKind::Normalizer, "normalizer");
    for descriptor in [true, false] {
        let mut changed = base.clone();
        let observed = changed.observed.as_mut().unwrap();
        if descriptor {
            observed.descriptor_digest = [9; 32];
        } else {
            observed.body_digest = [9; 32];
        }
        let mut r = resolver(vec![changed]);
        assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactDigestMismatch));
        assert_eq!(r.bound_count(), 0);
    }
    let mut changed = base;
    let observed = changed.observed.as_mut().unwrap();
    observed.body_length = 4;
    observed.body_digest = [9; 32];
    let mut r = resolver(vec![changed]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactLengthMismatch));
}

#[test]
fn v_r4_kind_schema_and_logical_identity() {
    let mut r = resolver(vec![node(1, ArtifactKind::Normalizer, "normalizer")]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Verification, archive()), Err(ArtifactError::ArtifactKindMismatch));
    r.supplied.get_mut(&reference(1)).unwrap().metadata.body_schema = 2;
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::UnsupportedArtifactSchema));
    for logical in ["stream/0", "stream/01", "stream/4294967296", "stream/+1"] {
        let identity = ArtifactIdentity {
            archive: archive(), kind: ArtifactKind::FeedProfile,
            logical: Token::new(logical).unwrap_or_else(|_| Token::new("invalid").unwrap()), revision: 1,
        };
        assert_eq!(identity.validate(), Err(ArtifactError::InvalidArtifactIdentity));
    }
    let mut value = node(2, ArtifactKind::Verification, "stream/1/raw/10");
    assert_eq!(value.metadata.validate(), Ok(()));
    value.metadata.identity.revision = 0;
    assert_eq!(value.metadata.validate(), Err(ArtifactError::InvalidArtifactIdentity));
}

#[test]
fn v_r4_duplicate_unsorted_and_wrong_dependency_set() {
    for dependencies in [vec![reference(1), reference(1)], vec![reference(2), reference(1)]] {
        let mut value = node(3, ArtifactKind::Config, "config");
        value.metadata.dependencies = dependencies.clone();
        value.required_dependencies = dependencies;
        let mut r = resolver(vec![value]);
        assert_eq!(r.resolve(reference(3), ArtifactKind::Config, archive()), Err(ArtifactError::InvalidArtifactDependencies));
    }
    let mut value = node(3, ArtifactKind::Config, "config");
    value.required_dependencies = vec![reference(1)];
    let mut r = resolver(vec![value]);
    assert_eq!(r.resolve(reference(3), ArtifactKind::Config, archive()), Err(ArtifactError::InvalidArtifactDependencies));
    let mut value = node(2, ArtifactKind::Config, "config");
    value.metadata.dependencies = vec![reference(1)];
    value.optional_basis = vec![reference(1)];
    let mut r = resolver(vec![value, node(1, ArtifactKind::Normalizer, "normalizer")]);
    assert_eq!(r.resolve(reference(2), ArtifactKind::Config, archive()), Err(ArtifactError::InvalidArtifactDependencies));
}

#[test]
fn v_r4_cycle_depth_and_closure_caps() {
    let mut first = node(1, ArtifactKind::Basis, "basis/1");
    let mut second = node(2, ArtifactKind::Basis, "basis/2");
    first.metadata.dependencies = vec![reference(2)];
    first.required_dependencies = vec![reference(2)];
    second.metadata.dependencies = vec![reference(1)];
    second.required_dependencies = vec![reference(1)];
    let mut r = resolver(vec![first, second]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Basis, archive()), Err(ArtifactError::ArtifactDependencyCycle));
    assert_eq!(r.bound_count(), 0);
    let mut nodes = Vec::new();
    for index in 1..=65 {
        let mut value = node(index, ArtifactKind::Basis, &format!("basis/{index}"));
        if index < 65 {
            value.metadata.dependencies = vec![reference(index + 1)];
            value.required_dependencies = value.metadata.dependencies.clone();
        }
        nodes.push(value);
    }
    let mut r = resolver(nodes);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Basis, archive()), Err(ArtifactError::ArtifactTooLarge));
    let last = r.supplied.get_mut(&reference(64)).unwrap();
    last.metadata.dependencies.clear();
    last.required_dependencies.clear();
    assert_eq!(r.resolve(reference(1), ArtifactKind::Basis, archive()).unwrap().reference(), reference(1));
    let mut root = node(1, ArtifactKind::Basis, "root");
    root.metadata.dependencies = (2..=65).map(reference).collect();
    root.required_dependencies = root.metadata.dependencies.clone();
    let mut nodes = vec![root];
    let mut next = 66;
    for index in 2..=65 {
        let mut child = node(index, ArtifactKind::Basis, &format!("branch/{index}"));
        for _ in 0..64 {
            let leaf = node(next, ArtifactKind::Basis, &format!("leaf/{next}"));
            child.metadata.dependencies.push(leaf.reference);
            nodes.push(leaf);
            next += 1;
        }
        child.required_dependencies = child.metadata.dependencies.clone();
        nodes.push(child);
    }
    let mut r = resolver(nodes);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Basis, archive()), Err(ArtifactError::ArtifactTooLarge));
    assert_eq!(r.bound_count(), 0);
}

#[test]
fn v_r4_descriptor_body_and_dependency_size_caps() {
    let base = node(1, ArtifactKind::Normalizer, "normalizer");
    let mut value = base.clone();
    value.descriptor_length = MAX_DESCRIPTOR_BYTES + 1;
    let mut r = resolver(vec![value]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactTooLarge));
    let mut value = base.clone();
    value.metadata.body_length = MAX_BODY_BYTES + 1;
    let mut r = resolver(vec![value]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactTooLarge));
    let mut value = base;
    value.metadata.dependencies = (2..=258).map(reference).collect();
    value.required_dependencies = value.metadata.dependencies.clone();
    let mut r = resolver(vec![value]);
    assert_eq!(r.resolve(reference(1), ArtifactKind::Normalizer, archive()), Err(ArtifactError::ArtifactTooLarge));
}
