//! Production-facing bounded filesystem WAL v1 storage.
//!
//! This crate implements the accepted PSRW frame codec, append-only file I/O,
//! physical archive recovery and honest storage watermarks. It deliberately
//! does not implement exchange networking, market reducers, artifact loading,
//! replay application or publication decisions.

#![forbid(unsafe_code)]

mod binary;
mod capture_session;
mod codec;
mod file;
mod recovery;
mod wire;

pub use binary::{CodecError, CodecErrorKind, Crc32, crc32};
pub use capture_session::{
    BoundedCaptureProfile, CaptureSessionOwner, DiagnosticCloseReport, DiagnosticCloseState,
    FinalizedArchive, LivePhysicalReport, MAX_BOOTSTRAP_BYTES, MAX_BOOTSTRAP_RECORDS,
    MAX_CAPTURE_PATH_BYTES, OwnerError, OwnerMemoryReport, SinkFault, SinkFaultKind,
};
pub use codec::{
    Definitions, FrameHeader, FrameView, HEADER_LEN, MAX_FRAME_LEN, MAX_PAYLOAD, decode_exact,
    decode_frame, encode_frame, scan_frame,
};
pub use file::{PrefixSummary, StorageWatermarks, WalWriter, WriterError};
pub use recovery::{
    ArchiveStatus, CanonicalStatus, Failure, FailureKind, LossError, PhysicalReport,
    RecoveryDiagnostic, ValidationError, WalReader,
};
