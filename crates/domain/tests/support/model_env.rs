//! Caller-owned immutable-prefix and SYNTHETIC artifact facts for the reference
//! model. Preloading this environment never activates a config or market effect.
//! No byte hashing, source verification, network or filesystem access occurs.

use std::collections::BTreeMap;

use domain::artifact::*;
use domain::event::*;
use domain::identity::*;
use domain::qualified::NumericSpec;
use domain::record::*;

use super::artifacts::SyntheticResolver;
use super::bodies::*;
use super::fixtures::MemoryPrefix;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedProof {
    pub record: RecordNo,
    pub reference: ArtifactRef,
    pub body: VerificationBody,
}

#[derive(Clone, Debug, Default)]
pub struct ModelEnv {
    pub resolver: SyntheticResolver,
    pub configs: BTreeMap<ArtifactRef, ConfigBody>,
    pub profiles: BTreeMap<ArtifactRef, FeedProfileBody>,
    pub instruments: BTreeMap<ArtifactRef, (InstrumentSlot, NumericSpec)>,
    pub verifications: BTreeMap<ArtifactRef, VerificationBody>,
    pub warmups: BTreeMap<ArtifactRef, WarmupBody>,
    pub freshness: BTreeMap<ArtifactRef, FreshnessBody>,
    pub normalizations: BTreeMap<RawFrameId, SyntheticNormalization>,
    pub prefix: MemoryPrefix,
    pub raw_records: BTreeMap<RawFrameId, RawInput>,
    pub specs: BTreeMap<(InstrumentSlot, SpecVersion), InstrumentSpecRecord>,
    pub first_proofs: BTreeMap<RawFrameId, AcceptedProof>,
}

impl ModelEnv {
    fn resolve_kind(
        &mut self,
        reference: ArtifactRef,
        kind: ArtifactKind,
        archive: ArchiveId,
        logical: &str,
        revision: Option<u32>,
        required: &[ArtifactRef],
    ) -> Result<(), ArtifactError> {
        self.resolver.resolve(reference, kind, archive)?;
        let supplied = self
            .resolver
            .supplied
            .get(&reference)
            .ok_or(ArtifactError::MissingArtifact)?;
        if supplied.metadata.identity.logical.as_str() != logical
            || revision.is_some_and(|r| supplied.metadata.identity.revision != r)
        {
            return Err(ArtifactError::ArtifactIdentityConflict);
        }
        // The typed body determines mandatory dependencies independently from
        // the descriptor's own list. Optional entries have already been checked
        // to be Basis artifacts by the synthetic resolver.
        supplied
            .metadata
            .validate_dependencies(required, &supplied.optional_basis)
    }

    pub fn config(
        &mut self,
        reference: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<ConfigBody, ArtifactError> {
        let body = self
            .configs
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        if body.proposal_revision != 2 {
            return Err(ArtifactError::UnsupportedArtifactSchema);
        }
        self.resolve_kind(
            reference,
            ArtifactKind::Config,
            archive,
            "config",
            Some(body.next.config.get()),
            &[body.normalizer_ref],
        )?;
        let normalizer = self
            .resolver
            .supplied
            .get(&body.normalizer_ref)
            .ok_or(ArtifactError::MissingArtifact)?;
        if normalizer.metadata.identity.kind != ArtifactKind::Normalizer {
            return Err(ArtifactError::ArtifactKindMismatch);
        }
        if normalizer.metadata.identity.revision != body.next.normalizer.get() {
            return Err(ArtifactError::ArtifactIdentityConflict);
        }
        Ok(body)
    }

    pub fn profile(
        &mut self,
        reference: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<FeedProfileBody, ArtifactError> {
        let body = self
            .profiles
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        if body.supported_normalizers.is_empty() || body.supported_normalizers.len() > 256 {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        validate_sorted_refs(&body.supported_normalizers)?;
        let mut required = body.supported_normalizers.clone();
        required.push(body.basis);
        required.sort_unstable();
        required.dedup();
        self.resolve_kind(
            reference,
            ArtifactKind::FeedProfile,
            archive,
            &format!("stream/{}", body.stream.get()),
            Some(body.version.get()),
            &required,
        )?;
        let node = &self.resolver.supplied[&reference];
        if !node.optional_basis.is_empty() {
            return Err(ArtifactError::InvalidArtifactDependencies);
        }
        let basis = self
            .resolver
            .supplied
            .get(&body.basis)
            .ok_or(ArtifactError::MissingArtifact)?;
        if basis.metadata.identity.kind != ArtifactKind::Basis {
            return Err(ArtifactError::ArtifactKindMismatch);
        }
        for dependency in &body.supported_normalizers {
            let node = self
                .resolver
                .supplied
                .get(dependency)
                .ok_or(ArtifactError::MissingArtifact)?;
            if node.metadata.identity.kind != ArtifactKind::Normalizer {
                return Err(ArtifactError::ArtifactKindMismatch);
            }
        }
        Ok(body)
    }

    pub fn instrument(
        &mut self,
        reference: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<(InstrumentSlot, NumericSpec), ArtifactError> {
        let body = self
            .instruments
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        self.resolve_kind(
            reference,
            ArtifactKind::InstrumentSpec,
            archive,
            &format!("instrument/{}", body.0.get()),
            Some(body.1.fields().reference.version.get()),
            &[],
        )?;
        Ok(body)
    }

    pub fn verification(
        &mut self,
        reference: ArtifactRef,
        config_ref: ArtifactRef,
        profile_ref: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<VerificationBody, ArtifactError> {
        let body = self
            .verifications
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        self.resolve_kind(
            reference,
            ArtifactKind::Verification,
            archive,
            &format!("stream/{}/raw/{}", body.scope.stream.get(), body.raw.get()),
            None,
            &[config_ref, profile_ref, body.continuity_basis],
        )?;
        let basis = self
            .resolver
            .supplied
            .get(&body.continuity_basis)
            .ok_or(ArtifactError::MissingArtifact)?;
        if basis.metadata.identity.kind != ArtifactKind::Basis {
            return Err(ArtifactError::ArtifactKindMismatch);
        }
        if !self.resolver.supplied[&reference].optional_basis.is_empty() {
            return Err(ArtifactError::InvalidArtifactDependencies);
        }
        Ok(body)
    }

    pub fn warmup(
        &mut self,
        reference: ArtifactRef,
        config_ref: ArtifactRef,
        profile_ref: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<WarmupBody, ArtifactError> {
        let body = self
            .warmups
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        self.resolve_kind(
            reference,
            ArtifactKind::Warmup,
            archive,
            &format!(
                "stream/{}/anchor/{}",
                body.scope.stream.get(),
                body.anchor.get()
            ),
            None,
            &[config_ref, profile_ref],
        )?;
        Ok(body)
    }

    pub fn freshness(
        &mut self,
        reference: ArtifactRef,
        config_ref: ArtifactRef,
        profile_ref: ArtifactRef,
        archive: ArchiveId,
    ) -> Result<FreshnessBody, ArtifactError> {
        let body = self
            .freshness
            .get(&reference)
            .cloned()
            .ok_or(ArtifactError::MissingArtifact)?;
        self.resolve_kind(
            reference,
            ArtifactKind::Freshness,
            archive,
            &format!(
                "stream/{}/basis/{}",
                body.scope.stream.get(),
                body.basis.get()
            ),
            None,
            &[config_ref, profile_ref],
        )?;
        Ok(body)
    }

    pub fn check_basis(
        &self,
        archive: ArchiveId,
        referring_record: RecordNo,
        records: &[RecordNo],
        required: &[RecordNo],
    ) -> Result<(), EventError> {
        if records.len() > 256 {
            return Err(EventError::EventTooLarge);
        }
        if records.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(EventError::SubEventOrderError);
        }
        for record in records {
            if *record >= referring_record {
                return Err(EventError::FutureCausalReference);
            }
            if !self.prefix.record_exists(RecordRef {
                archive,
                record: *record,
            }) {
                return Err(EventError::MissingCausalReference);
            }
        }
        if required.iter().any(|record| !records.contains(record)) {
            return Err(EventError::MissingCausalReference);
        }
        Ok(())
    }

    pub fn accept(&mut self, archive: ArchiveId, input: &RecordFrame, outputs: &[EventEnvelope]) {
        self.prefix.records.insert(RecordRef {
            archive,
            record: input.record_no,
        });
        match &input.value {
            Record::InstrumentSpec(spec) => {
                self.specs.insert(
                    (spec.slot, spec.numeric.fields().reference.version),
                    spec.clone(),
                );
            }
            Record::ConfigDefinition(config) => {
                self.prefix.configs.insert(config.next.config);
                if let Some(body) = self.configs.get(&config.evidence) {
                    self.prefix
                        .normalizers
                        .insert(config.next.normalizer, body.normalizer_ref);
                }
            }
            Record::RawInput(raw) => {
                let id = RawFrameId {
                    archive,
                    record: input.record_no,
                };
                self.raw_records.insert(id, raw.clone());
                if let Some(fact) = self.normalizations.get(&id) {
                    self.prefix.insert_frame(fact.frame.clone());
                }
            }
            _ => {}
        }
        for event in outputs {
            self.prefix.effects.insert(EventRef {
                archive,
                cursor: event.event_id.cursor,
            });
        }
    }
}
