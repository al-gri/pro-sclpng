//! Pure publication relations with explicitly SYNTHETIC storage completion.
//! No writer, publisher, filesystem call, live fence producer or delivery claim.

use domain::event::{EventId, InputCursor, RecordRef};
use domain::identity::{ArchiveId, CaptureSessionId, RecordNo, StreamId};
use domain::policy::{DurabilityMode, PolicyError, RecordingGate, WatermarkKind};
use domain::record::{Freshness, Reason, RecordingEvidence, RecordingHealth};

use super::bodies::EvidenceScope;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiptError {
    InvalidAcknowledgement,
    MissingWatermark,
    WatermarkRegression,
    WatermarkOrderError,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Watermarks {
    pub accepted: Option<RecordNo>,
    pub appended: Option<RecordNo>,
    pub written: Option<RecordNo>,
    pub flushed: Option<RecordNo>,
    pub durable: Option<RecordNo>,
}

impl Watermarks {
    pub fn get(&self, kind: WatermarkKind) -> Option<RecordNo> {
        match kind {
            WatermarkKind::Accepted => self.accepted,
            WatermarkKind::Appended => self.appended,
            WatermarkKind::Written => self.written,
            WatermarkKind::Flushed => self.flushed,
            WatermarkKind::Durable => self.durable,
        }
    }

    pub fn validate(&self) -> Result<(), ReceiptError> {
        let ordered = [
            self.accepted,
            self.appended,
            self.written,
            self.flushed,
            self.durable,
        ];
        for pair in ordered.windows(2) {
            match (pair[0], pair[1]) {
                (Some(weak), Some(strong)) if strong > weak => {
                    return Err(ReceiptError::WatermarkOrderError);
                }
                (None, Some(_)) => return Err(ReceiptError::WatermarkOrderError),
                _ => {}
            }
        }
        Ok(())
    }

    pub fn observe(
        &mut self,
        own: RecordNo,
        admitted_prefix: RecordNo,
        evidence: &RecordingEvidence,
    ) -> Result<(), ReceiptError> {
        self.validate()?;
        let Some(through) = evidence.through else {
            if evidence.health == RecordingHealth::Healthy {
                return Err(ReceiptError::MissingWatermark);
            }
            return Ok(());
        };
        if through >= own {
            return Err(ReceiptError::InvalidAcknowledgement);
        }
        if through > admitted_prefix {
            return Err(ReceiptError::WatermarkOrderError);
        }
        if self.get(evidence.kind).is_some_and(|old| through < old) {
            return Err(ReceiptError::WatermarkRegression);
        }
        let mut next = self.clone();
        // These are trusted synthetic observations. A stronger success includes
        // its weaker prefix; no weaker observation invents stronger completion.
        let raise = |slot: &mut Option<RecordNo>| {
            *slot = Some(slot.map_or(through, |old| old.max(through)));
        };
        raise(&mut next.accepted);
        match evidence.kind {
            WatermarkKind::Accepted => {}
            WatermarkKind::Appended => raise(&mut next.appended),
            WatermarkKind::Written => {
                raise(&mut next.appended);
                raise(&mut next.written);
            }
            WatermarkKind::Flushed => {
                raise(&mut next.appended);
                raise(&mut next.written);
                raise(&mut next.flushed);
            }
            WatermarkKind::Durable => {
                raise(&mut next.appended);
                raise(&mut next.written);
                raise(&mut next.flushed);
                raise(&mut next.durable);
            }
        }
        next.validate()?;
        *self = next;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordingSnapshot {
    pub health: RecordingHealth,
    pub reason: Reason,
    pub watermarks: Watermarks,
    pub last_receipt: Option<RecordNo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateProjection {
    pub scope: EvidenceScope,
    pub gate: RecordingGate,
    pub anchor: RecordNo,
    pub witness_record: RecordNo,
    /// Last applied batch references; the full causal prefix also includes all
    /// earlier updates. This reference model stores no production book levels.
    pub ordered_effects: Vec<EventId>,
    pub freshness: Freshness,
    pub recording: RecordingSnapshot,
    pub evaluation_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandidateId {
    pub archive: ArchiveId,
    pub creation_record: RecordNo,
    pub stream: StreamId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationCandidate {
    pub id: CandidateId,
    pub causal_frontier: RecordNo,
    pub available_at: InputCursor,
    pub projection: CandidateProjection,
    pub revoked: bool,
}

impl PublicationCandidate {
    pub fn freeze(at: RecordRef, projection: CandidateProjection) -> Self {
        Self {
            id: CandidateId {
                archive: at.archive,
                creation_record: at.record,
                stream: projection.scope.stream,
            },
            causal_frontier: at.record,
            available_at: at,
            projection,
            revoked: false,
        }
    }
}

/// Every field is supplied as an explicit synthetic assumption by a test.
/// This is NOT a receipt producer and is not exported by the domain crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticCompletion {
    pub archive: ArchiveId,
    pub session: CaptureSessionId,
    pub gate: RecordingGate,
    pub through: Option<RecordNo>,
    pub known_achieved_prefix: Option<RecordNo>,
    pub known_accepted_prefix: Option<RecordNo>,
    pub previous_reported_prefix: Option<RecordNo>,
    pub continuous_prefix: bool,
    pub explicitly_attested: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermitError {
    CanonicalBlocked,
    CandidateRevoked,
    DataNotUsable,
    RecordingNotHealthy,
    Configuration(PolicyError),
    FenceMissing,
    FenceBehindCandidate,
    FenceTooWeak,
    FenceScopeMismatch,
    FenceBeyondAchieved,
    UnverifiedStorageCompletion,
    WatermarkRegression,
}

pub struct PermitState<'a> {
    pub canonical_running: bool,
    pub usable_data: bool,
    pub recording: RecordingHealth,
    pub mode: DurabilityMode,
    pub scope: &'a EvidenceScope,
    pub current_candidate: Option<&'a PublicationCandidate>,
}

pub fn publication_permit(
    requested: &PublicationCandidate,
    state: &PermitState<'_>,
    completion: Option<&SyntheticCompletion>,
) -> Result<(), PermitError> {
    if !state.canonical_running {
        return Err(PermitError::CanonicalBlocked);
    }
    if requested.revoked
        || state.current_candidate != Some(requested)
        || requested.projection.scope != *state.scope
    {
        return Err(PermitError::CandidateRevoked);
    }
    if !state.usable_data {
        return Err(PermitError::DataNotUsable);
    }
    if state.recording != RecordingHealth::Healthy {
        return Err(PermitError::RecordingNotHealthy);
    }
    state
        .mode
        .validate_gate(requested.projection.gate)
        .map_err(PermitError::Configuration)?;
    let fence = completion.ok_or(PermitError::FenceMissing)?;
    if fence.archive != requested.id.archive || fence.session != state.scope.clock.session {
        return Err(PermitError::FenceScopeMismatch);
    }
    if !fence.gate.covers(requested.projection.gate) {
        return Err(PermitError::FenceTooWeak);
    }
    let (Some(through), Some(achieved), Some(accepted)) = (
        fence.through,
        fence.known_achieved_prefix,
        fence.known_accepted_prefix,
    ) else {
        return Err(PermitError::UnverifiedStorageCompletion);
    };
    if !fence.explicitly_attested || !fence.continuous_prefix {
        return Err(PermitError::UnverifiedStorageCompletion);
    }
    if fence
        .previous_reported_prefix
        .is_some_and(|previous| through < previous)
    {
        return Err(PermitError::WatermarkRegression);
    }
    if through > achieved || achieved > accepted {
        return Err(PermitError::FenceBeyondAchieved);
    }
    if through < requested.causal_frontier {
        return Err(PermitError::FenceBehindCandidate);
    }
    Ok(())
}
