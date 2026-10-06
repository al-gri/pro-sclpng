//! PSAD and PSAM memory encodings. Parsing does not compute SHA-256 or confer
//! authenticity/applicability. All helpers are local to the contract test binary.

use domain::artifact::*;
use domain::identity::{ArchiveId, Token};
use domain::record::ProvenanceKind;

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Descriptor {
    pub metadata: ArtifactMetadata,
    pub provenance: ProvenanceKind,
    pub provenance_text: String,
}

fn artifact_error(offset: usize, error: ArtifactError) -> Error {
    Error::new(offset, ErrorKind::Artifact(error))
}

pub fn decode_descriptor(bytes: &[u8], archive: ArchiveId) -> Result<Descriptor> {
    if bytes.len() > MAX_DESCRIPTOR_BYTES as usize {
        return Err(artifact_error(0, ArtifactError::ArtifactTooLarge));
    }
    let mut r = Reader::new(bytes, 0)?;
    if r.take(4)? != b"PSAD" {
        return Err(Error::new(0, ErrorKind::Corrupt("PSAD.magic")));
    }
    let format_version = r.u16()?;
    if format_version != 1 {
        return Err(artifact_error(4, ArtifactError::UnsupportedArtifactSchema));
    }
    let kind = checked(6, ArtifactKind::try_from(r.u8()?))?;
    let logical = r.token::<64>()?;
    let revision = r.u32()?;
    let body_schema = r.u16()?;
    let body_length = r.u64()?;
    let body_sha256 = r.array()?;
    let provenance = r.enum_tag("provenance_kind")?;
    let text_offset = r.offset();
    let text_length = usize::from(r.u16()?);
    r.count(text_length, 1, 1024)?;
    let text = r.take(text_length)?;
    if !text.iter().all(|byte| (0x20..=0x7e).contains(byte)) {
        return Err(Error::new(
            text_offset,
            ErrorKind::InvalidPayload("PSAD.provenance_text"),
        ));
    }
    let provenance_text = std::str::from_utf8(text)
        .expect("validated ASCII provenance")
        .to_owned();
    let count = usize::from(r.u16()?);
    r.count(count, 72, MAX_DEPENDENCIES)?;
    let mut dependencies = Vec::with_capacity(count);
    for _ in 0..count {
        dependencies.push(r.artifact()?);
    }
    r.finish()?;
    let metadata = ArtifactMetadata {
        identity: ArtifactIdentity {
            archive,
            kind,
            logical,
            revision,
        },
        format_version,
        body_schema,
        body_length,
        body_sha256,
        dependencies,
    };
    checked(0, metadata.validate())?;
    Ok(Descriptor {
        metadata,
        provenance,
        provenance_text,
    })
}

pub fn encode_descriptor(value: &Descriptor) -> Result<Vec<u8>> {
    checked(0, value.metadata.validate())?;
    let text = value.provenance_text.as_bytes();
    if text.len() > 1024 || !text.iter().all(|b| (0x20..=0x7e).contains(b)) {
        return Err(Error::new(
            0,
            ErrorKind::InvalidPayload("PSAD.provenance_text"),
        ));
    }
    let mut w = Writer::new(MAX_DESCRIPTOR_BYTES as usize);
    let m = &value.metadata;
    w.bytes(b"PSAD")?;
    w.u16(m.format_version)?;
    w.u8(m.identity.kind.tag())?;
    w.token(&m.identity.logical)?;
    w.u32(m.identity.revision)?;
    w.u16(m.body_schema)?;
    w.u64(m.body_length)?;
    w.bytes(&m.body_sha256)?;
    w.u8(value.provenance.tag())?;
    w.u16(u16::try_from(text.len()).expect("bounded text"))?;
    w.bytes(text)?;
    w.u16(u16::try_from(m.dependencies.len()).expect("bounded dependencies"))?;
    for reference in &m.dependencies {
        w.artifact(*reference)?;
    }
    Ok(w.finish())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestEntry {
    pub reference: ArtifactRef,
    pub descriptor_length: u32,
}

pub fn decode_manifest(bytes: &[u8]) -> Result<Vec<ManifestEntry>> {
    let mut r = Reader::new(bytes, 0)?;
    if r.take(4)? != b"PSAM" {
        return Err(Error::new(0, ErrorKind::Corrupt("PSAM.magic")));
    }
    if r.u16()? != 1 {
        return Err(artifact_error(4, ArtifactError::UnsupportedArtifactSchema));
    }
    let count = usize::try_from(r.u32()?).map_err(|_| Error::new(6, ErrorKind::LengthError))?;
    r.count(count, 76, MAX_CLOSURE)?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let reference = r.artifact()?;
        let offset = r.offset();
        let descriptor_length = r.u32()?;
        if u64::from(descriptor_length) > MAX_DESCRIPTOR_BYTES {
            return Err(artifact_error(offset, ArtifactError::ArtifactTooLarge));
        }
        entries.push(ManifestEntry {
            reference,
            descriptor_length,
        });
    }
    r.finish()?;
    let refs: Vec<_> = entries.iter().map(|entry| entry.reference).collect();
    checked(10, validate_sorted_refs(&refs))?;
    Ok(entries)
}

pub fn encode_manifest(entries: &[ManifestEntry]) -> Result<Vec<u8>> {
    if entries.len() > MAX_CLOSURE {
        return Err(artifact_error(6, ArtifactError::ArtifactTooLarge));
    }
    let refs: Vec<_> = entries.iter().map(|entry| entry.reference).collect();
    checked(10, validate_sorted_refs(&refs))?;
    let mut w = Writer::new(10 + MAX_CLOSURE * 76);
    w.bytes(b"PSAM")?;
    w.u16(1)?;
    w.u32(u32::try_from(entries.len()).expect("bounded manifest"))?;
    for entry in entries {
        if u64::from(entry.descriptor_length) > MAX_DESCRIPTOR_BYTES {
            return Err(artifact_error(0, ArtifactError::ArtifactTooLarge));
        }
        w.artifact(entry.reference)?;
        w.u32(entry.descriptor_length)?;
    }
    Ok(w.finish())
}

/// Only relates a declared manifest length to supplied bytes. Neither the hash
/// claim nor a matching length implies resolution or analytical activation.
pub fn check_manifest_length(entry: &ManifestEntry, actual: &[u8]) -> Result<()> {
    if usize::try_from(entry.descriptor_length).ok() != Some(actual.len()) {
        return Err(artifact_error(0, ArtifactError::ArtifactLengthMismatch));
    }
    Ok(())
}

pub fn logical(text: &str) -> Token<64> {
    Token::new(text).expect("static synthetic logical identity")
}
