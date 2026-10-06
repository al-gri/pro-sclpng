//! Production-facing bounded filesystem WAL v1 storage.
//!
//! This crate implements the accepted PSRW frame codec, append-only file I/O,
//! physical archive recovery and honest storage watermarks. It deliberately
//! does not implement exchange networking, market reducers, artifact loading,
//! replay application or publication decisions.

#![forbid(unsafe_code)]

mod binary;
mod codec;
mod file;
mod recovery;
mod wire;

pub use binary::{crc32, CodecError, CodecErrorKind, Crc32};
pub use codec::{
    decode_exact, decode_frame, encode_frame, scan_frame, Definitions, FrameHeader, FrameView,
    HEADER_LEN, MAX_FRAME_LEN, MAX_PAYLOAD,
};
pub use file::{StorageWatermarks, WalWriter, WriterError};
pub use recovery::{
    ArchiveStatus, CanonicalStatus, Failure, FailureKind, PhysicalReport, ReadArchive,
    ValidationError, WalReader, read_all,
};
