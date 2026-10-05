//! Content references and metadata guards, not a hash implementation or loader.
//!
//! A parsed ArtifactRef proves only its grammar. No API in this module creates
//! Verified evidence or fetches/executes artifact bytes. Synthetic resolution
//! and byte-layout helpers belong to integration-test support.

use std::fmt;
use std::str::FromStr;

use crate::identity::{ArchiveId, Token};

pub const MAX_DESCRIPTOR_BYTES: u64 = 65_536;
pub const MAX_BODY_BYTES: u64 = 16_777_216;
pub const MAX_DEPENDENCIES: usize = 256;
pub const MAX_CLOSURE: usize = 4096;
pub const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactError {
    InvalidArtifactRef,
    InvalidArtifactIdentity,
    MissingArtifact,
    ArtifactDigestMismatch,
    ArtifactLengthMismatch,
    ArtifactKindMismatch,
    UnsupportedArtifactSchema,
    ArtifactIdentityConflict,
    ArtifactDependencyCycle,
    ArtifactUnverified,
    ArtifactTooLarge,
    InvalidArtifactDependencies,
    UnsupportedNormalizerBinding,
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ArtifactError {}

type Result<T> = std::result::Result<T, ArtifactError>;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactRef([u8; 32]);

impl ArtifactRef {
    /// Returns the claimed digest, not a digest computed or verified here.
    pub const fn claimed_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

impl FromStr for ArtifactRef {
    type Err = ArtifactError;

    fn from_str(text: &str) -> Result<Self> {
        let bytes = text.as_bytes();
        if bytes.len() != 71 || !bytes.starts_with(b"sha256:") {
            return Err(ArtifactError::InvalidArtifactRef);
        }
        let mut digest = [0_u8; 32];
        for (slot, pair) in digest.iter_mut().zip(bytes[7..].chunks_exact(2)) {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            *slot = high * 16 + low;
        }
        Ok(Self(digest))
    }
}

fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ArtifactError::InvalidArtifactRef),
    }
}

impl fmt::Display for ArtifactRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sha256:")?;
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ArtifactKind {
    Normalizer = 1,
    Config = 2,
    FeedProfile = 3,
    InstrumentSpec = 4,
    Verification = 5,
    Warmup = 6,
    Freshness = 7,
    Basis = 8,
}

impl TryFrom<u8> for ArtifactKind {
    type Error = ArtifactError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Normalizer),
            2 => Ok(Self::Config),
            3 => Ok(Self::FeedProfile),
            4 => Ok(Self::InstrumentSpec),
            5 => Ok(Self::Verification),
            6 => Ok(Self::Warmup),
            7 => Ok(Self::Freshness),
            8 => Ok(Self::Basis),
            _ => Err(ArtifactError::ArtifactKindMismatch),
        }
    }
}

impl ArtifactKind {
    pub const fn tag(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ArtifactIdentity {
    pub archive: ArchiveId,
    pub kind: ArtifactKind,
    pub logical: Token<64>,
    pub revision: u32,
}

impl ArtifactIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.revision == 0 {
            return Err(ArtifactError::InvalidArtifactIdentity);
        }
        let parts: Vec<_> = self.logical.as_str().split('/').collect();
        let valid = match self.kind {
            ArtifactKind::Normalizer => parts == ["normalizer"],
            ArtifactKind::Config => parts == ["config"],
            ArtifactKind::FeedProfile => {
                parts.len() == 2 && parts[0] == "stream" && positive_id(parts[1], u32::MAX.into())
            }
            ArtifactKind::InstrumentSpec => {
                parts.len() == 2
                    && parts[0] == "instrument"
                    && positive_id(parts[1], u32::MAX.into())
            }
            ArtifactKind::Verification | ArtifactKind::Warmup | ArtifactKind::Freshness => {
                let middle = match self.kind {
                    ArtifactKind::Verification => "raw",
                    ArtifactKind::Warmup => "anchor",
                    _ => "basis",
                };
                parts.len() == 4
                    && parts[0] == "stream"
                    && positive_id(parts[1], u32::MAX.into())
                    && parts[2] == middle
                    && positive_id(parts[3], u64::MAX)
            }
            ArtifactKind::Basis => true,
        };
        if !valid {
            return Err(ArtifactError::InvalidArtifactIdentity);
        }
        Ok(())
    }
}

fn positive_id(text: &str, max: u64) -> bool {
    !text.is_empty()
        && !text.starts_with('0')
        && text.bytes().all(|b| b.is_ascii_digit())
        && text.parse::<u64>().is_ok_and(|n| n <= max)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactMetadata {
    pub identity: ArtifactIdentity,
    pub format_version: u16,
    pub body_schema: u16,
    pub body_length: u64,
    pub body_sha256: [u8; 32],
    pub dependencies: Vec<ArtifactRef>,
}

impl ArtifactMetadata {
    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.format_version != 1 || self.body_schema != 1 {
            return Err(ArtifactError::UnsupportedArtifactSchema);
        }
        if self.body_length > MAX_BODY_BYTES || self.dependencies.len() > MAX_DEPENDENCIES {
            return Err(ArtifactError::ArtifactTooLarge);
        }
        validate_sorted_refs(&self.dependencies)?;
        Ok(())
    }

    /// The required list comes from the typed body's recorded bindings, not
    /// from the descriptor's own untrusted dependency declaration.
    pub fn validate_dependencies(
        &self,
        required: &[ArtifactRef],
        optional_basis: &[ArtifactRef],
    ) -> Result<()> {
        self.validate()?;
        let mut expected = required.to_vec();
        expected.extend_from_slice(optional_basis);
        expected.sort_unstable();
        expected.dedup();
        if expected != self.dependencies {
            return Err(ArtifactError::InvalidArtifactDependencies);
        }
        Ok(())
    }
}

pub fn validate_sorted_refs(refs: &[ArtifactRef]) -> Result<()> {
    if refs.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ArtifactError::InvalidArtifactDependencies);
    }
    Ok(())
}

/// Reusing a reference is not redefining it. Same logical key plus different
/// content is a conflict, including a different body under a reused descriptor.
pub fn validate_identity_binding(
    old: (&ArtifactMetadata, ArtifactRef),
    new: (&ArtifactMetadata, ArtifactRef),
) -> Result<()> {
    if old.0.identity == new.0.identity && (old.1 != new.1 || old.0 != new.0) {
        return Err(ArtifactError::ArtifactIdentityConflict);
    }
    Ok(())
}
