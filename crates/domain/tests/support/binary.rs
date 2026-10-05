//! Bounded memory-only binary primitives for SPEC-001 test support.

use domain::artifact::ArtifactError;
use domain::event::EventError;
use domain::identity::IdentityError;
use domain::numeric::NumericError;
use domain::policy::PolicyError;
use domain::qualified::ValueError;
use domain::record::RecordError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    TruncatedTail,
    LengthError,
    Corrupt(&'static str),
    Unsupported { field: &'static str, value: u64 },
    InvalidPayload(&'static str),
    ChecksumMismatch,
    Identity(IdentityError),
    Numeric(NumericError),
    Value(ValueError),
    Artifact(ArtifactError),
    Policy(PolicyError),
    Event(EventError),
    Record(RecordError),
    UnknownDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    /// Offset in the supplied frame, descriptor or body, not a guessed prefix.
    pub offset: usize,
    pub kind: ErrorKind,
}

impl Error {
    pub fn new(offset: usize, kind: ErrorKind) -> Self {
        Self { offset, kind }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

macro_rules! wrap {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for ErrorKind {
            fn from(value: $ty) -> Self {
                Self::$variant(value)
            }
        }
    };
}

wrap!(IdentityError, Identity);
wrap!(NumericError, Numeric);
wrap!(ValueError, Value);
wrap!(ArtifactError, Artifact);
wrap!(PolicyError, Policy);
wrap!(EventError, Event);
wrap!(RecordError, Record);

pub fn checked<T, E>(offset: usize, value: std::result::Result<T, E>) -> Result<T>
where
    ErrorKind: From<E>,
{
    value.map_err(|error| Error::new(offset, error.into()))
}

pub struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
    base: usize,
}

macro_rules! read_integer {
    ($name:ident, $ty:ty, $size:literal) => {
        pub fn $name(&mut self) -> Result<$ty> {
            Ok(<$ty>::from_le_bytes(self.array::<$size>()?))
        }
    };
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8], base: usize) -> Result<Self> {
        base.checked_add(bytes.len())
            .ok_or_else(|| Error::new(base, ErrorKind::LengthError))?;
        Ok(Self { bytes, cursor: 0, base })
    }

    pub fn offset(&self) -> usize {
        self.base + self.cursor
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.cursor.checked_add(count)
            .ok_or_else(|| Error::new(self.offset(), ErrorKind::LengthError))?;
        let value = self.bytes.get(self.cursor..end)
            .ok_or_else(|| Error::new(self.offset(), ErrorKind::LengthError))?;
        self.cursor = end;
        Ok(value)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(N)?);
        Ok(value)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    read_integer!(u16, u16, 2);
    read_integer!(u32, u32, 4);
    read_integer!(u64, u64, 8);
    read_integer!(i64, i64, 8);
    read_integer!(u128, u128, 16);

    pub fn option<T>(&mut self, read: impl FnOnce(&mut Self) -> Result<T>) -> Result<Option<T>> {
        let offset = self.offset();
        match self.u8()? {
            0 => Ok(None),
            1 => read(self).map(Some),
            _ => Err(Error::new(offset, ErrorKind::InvalidPayload("option"))),
        }
    }

    pub fn bool(&mut self) -> Result<bool> {
        let offset = self.offset();
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::new(offset, ErrorKind::InvalidPayload("bool"))),
        }
    }

    /// Validate nested count * minimum entry size and cap before allocation.
    pub fn count(&self, count: usize, minimum: usize, cap: usize) -> Result<()> {
        let needed = count.checked_mul(minimum)
            .ok_or_else(|| Error::new(self.offset(), ErrorKind::LengthError))?;
        if count > cap || needed > self.remaining() {
            return Err(Error::new(self.offset(), ErrorKind::LengthError));
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(Error::new(self.offset(), ErrorKind::InvalidPayload("trailing_bytes")));
        }
        Ok(())
    }
}

pub struct Writer {
    bytes: Vec<u8>,
    cap: usize,
}

macro_rules! write_integer {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self, value: $ty) -> Result<()> {
            self.bytes(&value.to_le_bytes())
        }
    };
}

impl Writer {
    pub fn new(cap: usize) -> Self {
        Self { bytes: Vec::new(), cap }
    }

    pub fn bytes(&mut self, value: &[u8]) -> Result<()> {
        self.bytes.len().checked_add(value.len())
            .filter(|length| *length <= self.cap)
            .ok_or_else(|| Error::new(self.bytes.len(), ErrorKind::LengthError))?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    write_integer!(u8, u8);
    write_integer!(u16, u16);
    write_integer!(u32, u32);
    write_integer!(u64, u64);
    write_integer!(i64, i64);
    write_integer!(u128, u128);

    pub fn bool(&mut self, value: bool) -> Result<()> {
        self.u8(u8::from(value))
    }

    pub fn option<T>(&mut self, value: Option<T>, write: impl FnOnce(&mut Self, T) -> Result<()>) -> Result<()> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                write(self, value)
            }
        }
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// CRC-32/ISO-HDLC. No SHA, authentication or durability claims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Crc32(u32);

impl Default for Crc32 {
    fn default() -> Self {
        Self(0xffff_ffff)
    }
}

impl Crc32 {
    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                self.0 = (self.0 >> 1) ^ if self.0 & 1 == 1 { 0xedb8_8320 } else { 0 };
            }
        }
    }

    pub fn digest(self) -> u32 {
        !self.0
    }
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = Crc32::default();
    crc.update(bytes);
    crc.digest()
}

/// Parses static fixture hex, never manufactures expected bytes via the codec.
pub fn literal_hex(text: &str) -> Vec<u8> {
    let digits: String = text.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    assert!(digits.len().is_multiple_of(2), "malformed fixture hex");
    let (pairs, _) = digits.as_bytes().as_chunks::<2>();
    pairs.iter().map(|pair| {
        let value = std::str::from_utf8(pair).expect("ASCII fixture");
        u8::from_str_radix(value, 16).expect("hex fixture")
    }).collect()
}
