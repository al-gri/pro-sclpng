//! Typed bodies for memory-only descriptor tests and synthetic model inputs.
//! These do not load, hash, authenticate or execute production artifacts.

use domain::artifact::ArtifactRef;
use domain::event::{ActiveContext, ClockScope, NormalizedFrame};
use domain::identity::*;
use domain::policy::HealthPolicy;
use domain::record::{BookEvidenceKind, Freshness, ProvenanceKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigBody {
    pub proposal_revision: u16,
    pub next: ActiveContext,
    pub normalizer_ref: ArtifactRef,
    pub provenance: ProvenanceKind,
    pub policy: HealthPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeedProfileBody {
    pub stream: StreamId,
    pub instrument: InstrumentRef,
    pub channel: Channel,
    pub version: FeedProfileVersion,
    pub supported_normalizers: Vec<ArtifactRef>,
    pub basis: ArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceScope {
    pub archive: ArchiveId,
    pub clock: ClockScope,
    pub stream: StreamId,
    pub slot: InstrumentSlot,
    pub tag: EpochTag,
    pub context: ActiveContext,
    pub profile: FeedProfileVersion,
    pub barrier: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationBody {
    pub scope: EvidenceScope,
    pub raw: RecordNo,
    pub kind: BookEvidenceKind,
    pub raw_sample_ns: u64,
    pub not_before_ns: u64,
    pub valid_until_ns: Option<u64>,
    pub output_count: u32,
    pub output_sha256: [u8; 32],
    pub continuity_basis: ArtifactRef,
    pub basis_records: Vec<RecordNo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupBody {
    pub scope: EvidenceScope,
    pub anchor: RecordNo,
    pub update_count: u32,
    pub elapsed_ns: u64,
    pub observed_at_ns: u64,
    pub basis_records: Vec<RecordNo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshnessBody {
    pub scope: EvidenceScope,
    pub anchor: Option<RecordNo>,
    pub basis: RecordNo,
    pub freshness: Freshness,
    pub observed_at_ns: u64,
    pub valid_from_ns: Option<u64>,
    pub valid_until_ns: Option<u64>,
    pub basis_records: Vec<RecordNo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticNormalization {
    pub frame: NormalizedFrame,
    /// Deliberately supplied fixture commitment, not computed by this model.
    pub output_sha256: [u8; 32],
    /// Explicit synthetic profile assumption; never a real exchange guarantee.
    pub post_barrier_membership: bool,
}
