//! Pure SYNTHETIC resolution relation. The supplied digest observations are
//! explicit fixture assumptions, NOT SHA computation or production verification.
//! A parsed reference alone has no observation and cannot resolve as applicable.

use std::collections::{BTreeMap, BTreeSet};

use domain::artifact::*;
use domain::identity::ArchiveId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticBytes {
    pub descriptor_digest: [u8; 32],
    pub body_digest: [u8; 32],
    pub body_length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticArtifact {
    pub reference: ArtifactRef,
    pub metadata: ArtifactMetadata,
    pub descriptor_length: u64,
    pub observed: Option<SyntheticBytes>,
    pub required_dependencies: Vec<ArtifactRef>,
    pub optional_basis: Vec<ArtifactRef>,
    pub applicable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticApplicability {
    reference: ArtifactRef,
    kind: ArtifactKind,
    archive: ArchiveId,
}

impl SyntheticApplicability {
    pub fn reference(&self) -> ArtifactRef {
        self.reference
    }

    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub fn archive(&self) -> ArchiveId {
        self.archive
    }
}

#[derive(Clone, Debug, Default)]
pub struct SyntheticResolver {
    pub supplied: BTreeMap<ArtifactRef, SyntheticArtifact>,
    bindings: BTreeMap<ArtifactIdentity, (ArtifactMetadata, ArtifactRef)>,
}

impl SyntheticResolver {
    pub fn resolve(
        &mut self,
        reference: ArtifactRef,
        kind: ArtifactKind,
        archive: ArchiveId,
    ) -> Result<SyntheticApplicability, ArtifactError> {
        let root = self.supplied.get(&reference).ok_or(ArtifactError::MissingArtifact)?;
        if root.metadata.identity.kind != kind {
            return Err(ArtifactError::ArtifactKindMismatch);
        }
        let mut state = WalkState::default();
        self.visit(reference, archive, 1, &mut state)?;
        // Bind only after complete successful resolution; a failed closure must
        // not partially authorize future references or mutate prior bindings.
        self.bindings.extend(state.new_bindings);
        Ok(SyntheticApplicability { reference, kind, archive })
    }

    fn visit(
        &self,
        reference: ArtifactRef,
        archive: ArchiveId,
        depth: usize,
        state: &mut WalkState,
    ) -> Result<(), ArtifactError> {
        if depth > MAX_DEPTH {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        if state.visiting.contains(&reference) {
            return Err(ArtifactError::ArtifactDependencyCycle);
        }
        if state.done.contains(&reference) {
            return Ok(());
        }
        if state.done.len() + state.visiting.len() >= MAX_CLOSURE {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        let node = self.supplied.get(&reference).ok_or(ArtifactError::MissingArtifact)?;
        node.metadata.validate()?;
        if node.metadata.identity.archive != archive || node.reference != reference {
            return Err(ArtifactError::ArtifactIdentityConflict);
        }
        if node.descriptor_length > MAX_DESCRIPTOR_BYTES {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        if node.required_dependencies.len() > MAX_DEPENDENCIES
            || node.optional_basis.len() > MAX_DEPENDENCIES
        {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        node.metadata.validate_dependencies(&node.required_dependencies, &node.optional_basis)?;
        let observed = node.observed.as_ref().ok_or(ArtifactError::ArtifactUnverified)?;
        if observed.body_length != node.metadata.body_length {
            return Err(ArtifactError::ArtifactLengthMismatch);
        }
        if observed.descriptor_digest != *reference.claimed_digest()
            || observed.body_digest != node.metadata.body_sha256
        {
            return Err(ArtifactError::ArtifactDigestMismatch);
        }
        if !node.applicable {
            return Err(ArtifactError::ArtifactUnverified);
        }
        for dependency in &node.optional_basis {
            let basis = self.supplied.get(dependency).ok_or(ArtifactError::MissingArtifact)?;
            if basis.metadata.identity.kind != ArtifactKind::Basis {
                return Err(ArtifactError::InvalidArtifactDependencies);
            }
        }
        if let Some((old, old_ref)) = self.bindings.get(&node.metadata.identity) {
            validate_identity_binding((old, *old_ref), (&node.metadata, reference))?;
        }
        if let Some((old, old_ref)) = state.new_bindings.get(&node.metadata.identity) {
            validate_identity_binding((old, *old_ref), (&node.metadata, reference))?;
        }
        state.visiting.insert(reference);
        state.new_bindings.insert(
            node.metadata.identity.clone(),
            (node.metadata.clone(), reference),
        );
        for dependency in &node.metadata.dependencies {
            self.visit(*dependency, archive, depth + 1, state)?;
        }
        state.visiting.remove(&reference);
        state.done.insert(reference);
        Ok(())
    }

    pub fn bound_count(&self) -> usize {
        self.bindings.len()
    }
}

#[derive(Default)]
struct WalkState {
    visiting: BTreeSet<ArtifactRef>,
    done: BTreeSet<ArtifactRef>,
    new_bindings: BTreeMap<ArtifactIdentity, (ArtifactMetadata, ArtifactRef)>,
}
