use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use domain::identity::RecordNo;
use domain::record::RecordFrame;

use crate::binary::CodecError;
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
}

impl fmt::Display for WriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "{error}"),
            Self::Validation(error) => write!(f, "{error}"),
            Self::Io { operation, source } => write!(f, "{operation}: {source}"),
            Self::Poisoned => f.write_str("WAL writer is poisoned after a failed storage operation"),
            Self::Closed => f.write_str("WAL writer is already finalized"),
            Self::RotationRequired => {
                f.write_str("WAL rotation requires a non-final SegmentSeal")
            }
            Self::ArchiveNotSealed => {
                f.write_str("durable finalization requires an accepted ArchiveSeal")
            }
            Self::OffsetOverflow => f.write_str("WAL file offset overflow"),
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
    writer: BufWriter<File>,
    creation_metadata_synced: bool,
}

impl FileBackend {
    fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            creation_metadata_synced: false,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        if !self.creation_metadata_synced {
            sync_parent_directory(&self.path)?;
            self.creation_metadata_synced = true;
        }
        Ok(())
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
        })
    }

    pub fn path(&self) -> &Path {
        &self.backend.path
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

    pub fn append(&mut self, frame: &RecordFrame) -> Result<StorageWatermarks, WriterError> {
        self.ensure_writable()?;
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
        if !self.validator.can_rotate() {
            return Err(WriterError::RotationRequired);
        }
        self.sync_all()?;
        let next_backend = FileBackend::create(next_path.as_ref()).map_err(|source| {
            WriterError::Io {
                operation: "create_segment",
                source,
            }
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
