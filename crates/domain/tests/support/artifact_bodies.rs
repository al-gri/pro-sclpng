//! Typed artifact body bytes, not an artifact loader or verifier.

use domain::artifact::{ArtifactError, ArtifactKind, MAX_BODY_BYTES, validate_sorted_refs};
use domain::event::ClockScope;
use domain::identity::*;
use domain::policy::{DurabilityMode, HealthPolicy};
use domain::qualified::NumericSpec;

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked};
use super::bodies::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Body {
    Opaque(Vec<u8>),
    Config(ConfigBody),
    FeedProfile(FeedProfileBody),
    InstrumentSpec(InstrumentSlot, NumericSpec),
    Verification(VerificationBody),
    Warmup(WarmupBody),
    Freshness(FreshnessBody),
}

impl Reader<'_> {
    pub fn evidence_scope(&mut self) -> Result<EvidenceScope> {
        Ok(EvidenceScope {
            archive: self.archive()?,
            clock: ClockScope { session: self.session()?, clock: self.clock()? },
            stream: self.stream()?,
            slot: self.slot()?,
            tag: self.epoch_tag()?,
            context: self.active_context()?,
            profile: self.profile_version()?,
            barrier: self.u64()?,
        })
    }

    pub fn basis_records(&mut self) -> Result<Vec<RecordNo>> {
        let offset = self.offset();
        let count = usize::from(self.u16()?);
        self.count(count, 8, 256)?;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(self.record_no()?);
        }
        if records.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::new(offset, ErrorKind::InvalidPayload("BasisRecords.order")));
        }
        Ok(records)
    }
}

impl Writer {
    pub fn evidence_scope(&mut self, scope: &EvidenceScope) -> Result<()> {
        self.bytes(scope.archive.as_bytes())?;
        self.bytes(scope.clock.session.as_bytes())?;
        self.u32(scope.clock.clock.get())?;
        self.u32(scope.stream.get())?;
        self.u32(scope.slot.get())?;
        self.epoch_tag(scope.tag)?;
        self.active_context(scope.context)?;
        self.u32(scope.profile.get())?;
        self.u64(scope.barrier)
    }

    pub fn basis_records(&mut self, records: &[RecordNo]) -> Result<()> {
        if records.len() > 256 {
            return Err(Error::new(0, ErrorKind::LengthError));
        }
        if records.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::new(0, ErrorKind::InvalidPayload("BasisRecords.order")));
        }
        self.u16(u16::try_from(records.len()).expect("bounded basis"))?;
        for record in records {
            self.u64(record.get())?;
        }
        Ok(())
    }
}

pub fn decode_body(kind: ArtifactKind, bytes: &[u8]) -> Result<Body> {
    if bytes.len() > MAX_BODY_BYTES as usize {
        return Err(Error::new(0, ErrorKind::Artifact(ArtifactError::ArtifactTooLarge)));
    }
    let mut r = Reader::new(bytes, 0)?;
    let body = match kind {
        ArtifactKind::Normalizer | ArtifactKind::Basis => Body::Opaque(r.take(bytes.len())?.to_vec()),
        ArtifactKind::Config => {
            let proposal_revision = r.u16()?;
            if proposal_revision != 2 {
                return Err(Error::new(0, ErrorKind::Artifact(ArtifactError::UnsupportedArtifactSchema)));
            }
            let next = r.active_context()?;
            let normalizer_ref = r.artifact()?;
            let provenance = r.enum_tag("provenance_kind")?;
            let policy = HealthPolicy {
                fields: r.policy_fields()?,
                pending_max_frames: r.u32()?,
                pending_max_raw_bytes: r.u64()?,
                pending_max_outputs: r.u32()?,
                pending_wait_ns: r.u64()?,
                quiet_max_lifetime_ns: r.option(Reader::u64)?,
            };
            checked(0, policy.validate(DurabilityMode::Buffered))?;
            Body::Config(ConfigBody { proposal_revision, next, normalizer_ref, provenance, policy })
        }
        ArtifactKind::FeedProfile => {
            let stream = r.stream()?;
            let instrument = r.instrument()?;
            let channel = r.enum_tag("Channel")?;
            let version = r.profile_version()?;
            let count = usize::from(r.u16()?);
            if count == 0 {
                return Err(Error::new(r.offset() - 2, ErrorKind::LengthError));
            }
            r.count(count, 72, 256)?;
            let mut supported_normalizers = Vec::with_capacity(count);
            for _ in 0..count {
                supported_normalizers.push(r.artifact()?);
            }
            checked(0, validate_sorted_refs(&supported_normalizers))?;
            Body::FeedProfile(FeedProfileBody {
                stream,
                instrument,
                channel,
                version,
                supported_normalizers,
                basis: r.artifact()?,
            })
        }
        ArtifactKind::InstrumentSpec => {
            let (slot, spec) = r.numeric_spec()?;
            Body::InstrumentSpec(slot, spec)
        }
        ArtifactKind::Verification => Body::Verification(VerificationBody {
            scope: r.evidence_scope()?,
            raw: r.record_no()?,
            kind: r.enum_tag("Verification.evidence_kind")?,
            raw_sample_ns: r.u64()?,
            not_before_ns: r.u64()?,
            valid_until_ns: r.option(Reader::u64)?,
            output_count: r.u32()?,
            output_sha256: r.array()?,
            continuity_basis: r.artifact()?,
            basis_records: r.basis_records()?,
        }),
        ArtifactKind::Warmup => Body::Warmup(WarmupBody {
            scope: r.evidence_scope()?,
            anchor: r.record_no()?,
            update_count: r.u32()?,
            elapsed_ns: r.u64()?,
            observed_at_ns: r.u64()?,
            basis_records: r.basis_records()?,
        }),
        ArtifactKind::Freshness => Body::Freshness(FreshnessBody {
            scope: r.evidence_scope()?,
            anchor: r.option(Reader::record_no)?,
            basis: r.record_no()?,
            freshness: r.enum_tag("FreshnessEvidence.freshness")?,
            observed_at_ns: r.u64()?,
            valid_from_ns: r.option(Reader::u64)?,
            valid_until_ns: r.option(Reader::u64)?,
            basis_records: r.basis_records()?,
        }),
    };
    r.finish()?;
    Ok(body)
}

pub fn encode_body(value: &Body) -> Result<Vec<u8>> {
    let mut w = Writer::new(MAX_BODY_BYTES as usize);
    match value {
        Body::Opaque(bytes) => w.bytes(bytes)?,
        Body::Config(v) => {
            if v.proposal_revision != 2 {
                return Err(Error::new(0, ErrorKind::Artifact(ArtifactError::UnsupportedArtifactSchema)));
            }
            checked(0, v.policy.validate(DurabilityMode::Buffered))?;
            w.u16(v.proposal_revision)?;
            w.active_context(v.next)?;
            w.artifact(v.normalizer_ref)?;
            w.u8(v.provenance.tag())?;
            w.policy_fields(v.policy.fields)?;
            w.u32(v.policy.pending_max_frames)?;
            w.u64(v.policy.pending_max_raw_bytes)?;
            w.u32(v.policy.pending_max_outputs)?;
            w.u64(v.policy.pending_wait_ns)?;
            w.option(v.policy.quiet_max_lifetime_ns, Writer::u64)?;
        }
        Body::FeedProfile(v) => {
            if v.supported_normalizers.is_empty() || v.supported_normalizers.len() > 256 {
                return Err(Error::new(0, ErrorKind::LengthError));
            }
            checked(0, validate_sorted_refs(&v.supported_normalizers))?;
            w.u32(v.stream.get())?;
            w.instrument(&v.instrument)?;
            w.u8(v.channel.tag())?;
            w.u32(v.version.get())?;
            w.u16(u16::try_from(v.supported_normalizers.len()).expect("bounded normalizers"))?;
            for reference in &v.supported_normalizers {
                w.artifact(*reference)?;
            }
            w.artifact(v.basis)?;
        }
        Body::InstrumentSpec(slot, spec) => w.numeric_spec(*slot, spec)?,
        Body::Verification(v) => {
            w.evidence_scope(&v.scope)?;
            w.u64(v.raw.get())?;
            w.u8(v.kind.tag())?;
            w.u64(v.raw_sample_ns)?;
            w.u64(v.not_before_ns)?;
            w.option(v.valid_until_ns, Writer::u64)?;
            w.u32(v.output_count)?;
            w.bytes(&v.output_sha256)?;
            w.artifact(v.continuity_basis)?;
            w.basis_records(&v.basis_records)?;
        }
        Body::Warmup(v) => {
            w.evidence_scope(&v.scope)?;
            w.u64(v.anchor.get())?;
            w.u32(v.update_count)?;
            w.u64(v.elapsed_ns)?;
            w.u64(v.observed_at_ns)?;
            w.basis_records(&v.basis_records)?;
        }
        Body::Freshness(v) => {
            w.evidence_scope(&v.scope)?;
            w.option(v.anchor, |w, record| w.u64(record.get()))?;
            w.u64(v.basis.get())?;
            w.u8(v.freshness.tag())?;
            w.u64(v.observed_at_ns)?;
            w.option(v.valid_from_ns, Writer::u64)?;
            w.option(v.valid_until_ns, Writer::u64)?;
            w.basis_records(&v.basis_records)?;
        }
    }
    Ok(w.finish())
}
