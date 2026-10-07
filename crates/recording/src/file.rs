use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use domain::capture_session::{AuthorityError, CaptureSessionAuthority};
use domain::identity::RecordNo;
use domain::record::{Record, RecordFrame};

use crate::binary::{CodecError, Crc32};
use crate::codec::{encode_frame, scan_frame};
use crate::recovery::{ArchiveValidator, ValidationError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageWatermarks {
    pub accepted: Option<RecordNo>,
    pub appended: Option<RecordNo>,
    pub written: Option<RecordNo>,
    pub flushed: Option<RecordNo>,
    pub durable: Option<RecordNo>,
}

/// Fixed-size summary of successfully written frames. It does not retain a
/// second copy of the archive and is independently checked by the WAL reader.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrefixSummary {
    pub frame_count: u64,
    pub physical_bytes: u64,
    pub crc32: u32,
    pub prior_record: Option<RecordNo>,
    pub has_gap: bool,
}

impl StorageWatermarks {
    fn accepted(&mut self, record: RecordNo) {
        self.accepted = Some(record);
        self.appended = Some(record);
    }

    fn written(&mut self, record: RecordNo) {
        self.written = Some(record);
    }

    fn flushed(&mut self) {
        self.flushed = self.written;
    }

    fn durable(&mut self) {
        self.durable = self.flushed;
    }
}

#[derive(Debug)]
pub enum WriterError {
    Codec(CodecError),
    Validation(ValidationError),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    Poisoned,
    Closed,
    RotationRequired,
    ArchiveNotSealed,
    OffsetOverflow,
    Session(AuthorityError),
    BoundRotationForbidden,
    CanonicalSealRequired,
}

impl fmt::Display for WriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "{error}"),
            Self::Validation(error) => write!(f, "{error}"),
            Self::Io { operation, source } => write!(f, "{operation}: {source}"),
            Self::Poisoned => {
                f.write_str("WAL writer is poisoned after a failed storage operation")
            }
            Self::Closed => f.write_str("WAL writer is already finalized"),
            Self::RotationRequired => f.write_str("WAL rotation requires a non-final SegmentSeal"),
            Self::ArchiveNotSealed => {
                f.write_str("durable finalization requires an accepted ArchiveSeal")
            }
            Self::OffsetOverflow => f.write_str("WAL file offset overflow"),
            Self::Session(error) => write!(f, "capture session: {error:?}"),
            Self::BoundRotationForbidden => {
                f.write_str("bounded capture sessions have one segment")
            }
            Self::CanonicalSealRequired => {
                f.write_str("capture seals require the exclusive owner finalization boundary")
            }
        }
    }
}

impl std::error::Error for WriterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Validation(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<CodecError> for WriterError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<ValidationError> for WriterError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

struct FileBackend {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    creation_metadata_synced: bool,
    suppress_flush_on_drop: bool,
}

impl Drop for FileBackend {
    fn drop(&mut self) {
        if self.suppress_flush_on_drop {
            self.close_without_retry();
        }
    }
}

impl FileBackend {
    fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: Some(BufWriter::new(file)),
            creation_metadata_synced: false,
            suppress_flush_on_drop: false,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer_mut()?.write_all(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer_mut()?.flush()
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.writer_mut()?.flush()?;
        self.writer_mut()?.get_ref().sync_all()?;
        if !self.creation_metadata_synced {
            sync_parent_directory(&self.path)?;
            self.creation_metadata_synced = true;
        }
        Ok(())
    }

    fn writer_mut(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "closed WAL descriptor"))
    }

    fn close_without_retry(&mut self) {
        if let Some(writer) = self.writer.take() {
            // into_parts never flushes. A stopped write must not be retried by
            // BufWriter::drop, nor may discarded buffering be called durable.
            let (file, buffer) = writer.into_parts();
            drop(buffer);
            drop(file);
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable WAL creation metadata sync is only implemented for Unix targets",
    ))
}

/// Append-only WAL writer for one archive.
///
/// Files are created with create_new and never reopened for append. Restart is
/// therefore a new archive. The writer never reorders records: successful
/// writes follow the exact order of append calls and dense RecordNo validation.
pub struct WalWriter {
    backend: FileBackend,
    validator: ArchiveValidator,
    segment_index: usize,
    local_offset: u64,
    absolute_offset: u64,
    watermarks: StorageWatermarks,
    poisoned: bool,
    closed: bool,
    prefix_count: u64,
    prefix_crc: Crc32,
    prefix_has_gap: bool,
    capture_authority: Option<CaptureSessionAuthority>,
    owner_seal_authorized: bool,
}

impl WalWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            backend: FileBackend::create(path.as_ref())?,
            validator: ArchiveValidator::default(),
            segment_index: 0,
            local_offset: 0,
            absolute_offset: 0,
            watermarks: StorageWatermarks::default(),
            poisoned: false,
            closed: false,
            prefix_count: 0,
            prefix_crc: Crc32::default(),
            prefix_has_gap: false,
            capture_authority: None,
            owner_seal_authorized: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.backend.path
    }

    pub(crate) fn bind_capture_session(&mut self, authority: CaptureSessionAuthority) {
        self.capture_authority = Some(authority);
        self.backend.suppress_flush_on_drop = true;
    }

    pub(crate) fn authorize_owner_seals(&mut self) -> Result<(), WriterError> {
        let authority = self
            .capture_authority
            .as_ref()
            .ok_or(WriterError::CanonicalSealRequired)?;
        authority
            .ensure_finalization_authorized()
            .map_err(WriterError::Session)?;
        self.owner_seal_authorized = true;
        Ok(())
    }

    pub const fn watermarks(&self) -> StorageWatermarks {
        self.watermarks
    }

    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    pub const fn prefix_summary(&self) -> PrefixSummary {
        PrefixSummary {
            frame_count: self.prefix_count,
            physical_bytes: self.absolute_offset,
            crc32: self.prefix_crc.digest(),
            prior_record: self.watermarks.written,
            has_gap: self.prefix_has_gap,
        }
    }

    pub(crate) fn backend_capacity(&self) -> usize {
        self.backend.path.capacity() + self.backend.writer.as_ref().map_or(0, BufWriter::capacity)
    }

    /// Close a diagnostic descriptor without producing either seal. Even a
    /// poisoned writer may be closed, but it is never resumed or finalized.
    pub(crate) fn close_diagnostic(&mut self) -> Result<StorageWatermarks, WriterError> {
        if self.closed {
            return Ok(self.watermarks);
        }
        let result = if self.poisoned {
            Err(WriterError::Poisoned)
        } else {
            self.sync_all()
        };
        self.backend.close_without_retry();
        self.closed = true;
        result
    }

    pub fn append(&mut self, frame: &RecordFrame) -> Result<StorageWatermarks, WriterError> {
        self.ensure_writable()?;
        if let Some(authority) = &self.capture_authority {
            if matches!(frame.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_))
                && !self.owner_seal_authorized
            {
                return Err(WriterError::CanonicalSealRequired);
            }
            authority
                .permit_writer(frame.value.kind())
                .map_err(WriterError::Session)?;
            if matches!(
                frame.value,
                Record::ArchiveStart(_)
                    | Record::InstrumentSpec(_)
                    | Record::StreamDefinition(_)
                    | Record::ConfigDefinition(_)
                    | Record::SegmentStart(_)
            ) {
                return Err(WriterError::BoundRotationForbidden);
            }
            if matches!(&frame.value, Record::SegmentSeal(seal) if !seal.is_final) {
                return Err(WriterError::BoundRotationForbidden);
            }
        }
        let bytes = encode_frame(frame)?;
        let view = scan_frame(&bytes, self.absolute_offset)?;
        let protected_end = bytes
            .len()
            .checked_sub(4)
            .ok_or(WriterError::OffsetOverflow)?;

        let mut candidate = self.validator.clone();
        candidate.accept(
            frame,
            self.segment_index,
            self.local_offset,
            self.absolute_offset,
            &bytes[..protected_end],
            view.checksum,
        )?;

        self.watermarks.accepted(frame.record_no);
        if let Err(source) = self.backend.write_all(&bytes) {
            self.poisoned = true;
            return Err(WriterError::Io {
                operation: "write_all",
                source,
            });
        }
        self.watermarks.written(frame.record_no);

        let frame_len = u64::try_from(bytes.len()).map_err(|_| WriterError::OffsetOverflow)?;
        self.prefix_count = self
            .prefix_count
            .checked_add(1)
            .ok_or(WriterError::OffsetOverflow)?;
        self.prefix_crc.update(&bytes[..protected_end]);
        self.prefix_has_gap |= matches!(frame.value, Record::Gap(_));
        self.local_offset = self
            .local_offset
            .checked_add(frame_len)
            .ok_or(WriterError::OffsetOverflow)?;
        self.absolute_offset = self
            .absolute_offset
            .checked_add(frame_len)
            .ok_or(WriterError::OffsetOverflow)?;
        self.validator = candidate;
        Ok(self.watermarks)
    }

    /// Flush userspace buffering to the operating system.
    ///
    /// A successful flush is not advertised as power-loss durability.
    pub fn flush(&mut self) -> Result<StorageWatermarks, WriterError> {
        self.ensure_writable()?;
        if let Err(source) = self.backend.flush() {
            self.poisoned = true;
            return Err(WriterError::Io {
                operation: "flush",
                source,
            });
        }
        self.watermarks.flushed();
        Ok(self.watermarks)
    }

    /// Flush and invoke File::sync_all plus Unix parent-directory sync for
    /// creation metadata. This records successful platform operations only; it
    /// does not make a filesystem-independent power-loss claim.
    pub fn sync_all(&mut self) -> Result<StorageWatermarks, WriterError> {
        self.ensure_writable()?;
        if let Err(source) = self.backend.sync_all() {
            self.poisoned = true;
            return Err(WriterError::Io {
                operation: "sync_all",
                source,
            });
        }
        self.watermarks.flushed();
        self.watermarks.durable();
        Ok(self.watermarks)
    }

    /// Rotate to a caller-selected new segment path.
    ///
    /// The previous segment must end with a non-final SegmentSeal. Rotation
    /// syncs the old file and its creation metadata before creating the next
    /// file. The next append is required to be the matching SegmentStart.
    pub fn rotate(&mut self, next_path: impl AsRef<Path>) -> Result<(), WriterError> {
        self.ensure_writable()?;
        if self.capture_authority.is_some() {
            return Err(WriterError::BoundRotationForbidden);
        }
        if !self.validator.can_rotate() {
            return Err(WriterError::RotationRequired);
        }
        self.sync_all()?;
        let next_backend =
            FileBackend::create(next_path.as_ref()).map_err(|source| WriterError::Io {
                operation: "create_segment",
                source,
            })?;
        self.backend = next_backend;
        self.segment_index = self
            .segment_index
            .checked_add(1)
            .ok_or(WriterError::OffsetOverflow)?;
        self.local_offset = 0;
        Ok(())
    }

    /// Finalize an already physically sealed archive with a successful sync.
    ///
    /// ArchiveSeal must already have been appended. No automatic seal fields
    /// are invented by this API.
    pub fn finish(&mut self) -> Result<StorageWatermarks, WriterError> {
        self.ensure_writable()?;
        if let Some(authority) = &self.capture_authority {
            if !self.owner_seal_authorized {
                return Err(WriterError::CanonicalSealRequired);
            }
            authority
                .ensure_finalization_authorized()
                .map_err(WriterError::Session)?;
        }
        if !self.validator.archive_sealed() {
            return Err(WriterError::ArchiveNotSealed);
        }
        let watermarks = self.sync_all()?;
        self.closed = true;
        Ok(watermarks)
    }

    fn ensure_writable(&self) -> Result<(), WriterError> {
        if self.closed {
            return Err(WriterError::Closed);
        }
        if self.poisoned {
            return Err(WriterError::Poisoned);
        }
        if let Some(authority) = &self.capture_authority {
            authority
                .ensure_storage_writable()
                .map_err(WriterError::Session)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::StorageWatermarks;
    use domain::identity::RecordNo;

    #[test]
    fn watermarks_advance_only_at_explicit_success_boundaries() {
        let first = RecordNo::new(1).expect("positive");
        let mut marks = StorageWatermarks::default();
        marks.accepted(first);
        assert_eq!(marks.accepted, Some(first));
        assert_eq!(marks.appended, Some(first));
        assert_eq!(marks.written, None);
        assert_eq!(marks.flushed, None);
        assert_eq!(marks.durable, None);

        marks.written(first);
        assert_eq!(marks.written, Some(first));
        assert_eq!(marks.flushed, None);
        marks.flushed();
        assert_eq!(marks.flushed, Some(first));
        assert_eq!(marks.durable, None);
        marks.durable();
        assert_eq!(marks.durable, Some(first));
    }
}
