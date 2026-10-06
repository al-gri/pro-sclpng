//! Qualified identities and checked counters. Equality never folds symbol case.

use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityError {
    InvalidToken,
    ZeroIdentifier(&'static str),
    CounterExhausted(&'static str),
    UnsupportedTag { field: &'static str, value: u8 },
    IdentityMismatch,
    SpecMismatch,
    IdentityConflict,
    EpochMismatch,
    EpochRollback,
    InvalidChannelBinding,
    WriterRebindRequiresNewArchive,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for IdentityError {}

type Result<T> = std::result::Result<T, IdentityError>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Token<const N: usize>(String);

impl<const N: usize> Token<N> {
    pub fn new(text: &str) -> Result<Self> {
        if N == 0 || N > 128 || text.is_empty() || text.len() > N {
            return Err(IdentityError::InvalidToken);
        }
        if !text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'/' | b'-'))
        {
            return Err(IdentityError::InvalidToken);
        }
        Ok(Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<const N: usize> FromStr for Token<N> {
    type Err = IdentityError;

    fn from_str(text: &str) -> Result<Self> {
        Self::new(text)
    }
}

impl<const N: usize> fmt::Display for Token<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! positive_id {
    ($name:ident, $integer:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($integer);

        impl $name {
            pub fn new(value: $integer) -> Result<Self> {
                if value == 0 {
                    return Err(IdentityError::ZeroIdentifier(stringify!($name)));
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> $integer {
                self.0
            }

            pub fn checked_next(self) -> Result<Self> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(IdentityError::CounterExhausted(stringify!($name)))
            }
        }
    };
}

positive_id!(SpecVersion, u32);
positive_id!(ConfigVersion, u32);
positive_id!(NormalizerVersion, u32);
positive_id!(FeedProfileVersion, u32);
positive_id!(InstrumentSlot, u32);
positive_id!(StreamId, u32);
positive_id!(BookId, u32);
positive_id!(ConnectionId, u32);
positive_id!(ClockId, u32);
positive_id!(ConnectionEpoch, u64);
positive_id!(SubscriptionEpoch, u64);
positive_id!(BookEpoch, u64);
positive_id!(RecordNo, u64);
positive_id!(CaptureAttemptNo, u64);

macro_rules! index {
    ($name:ident, $integer:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($integer);

        impl $name {
            pub const fn new(value: $integer) -> Self {
                Self(value)
            }

            pub const fn get(self) -> $integer {
                self.0
            }

            pub fn checked_next(self) -> Result<Self> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(IdentityError::CounterExhausted(stringify!($name)))
            }
        }
    };
}

index!(RawSubIndex, u32);
index!(OutputIndex, u32);
index!(SegmentNo, u32);
index!(MonotonicNs, u64);
index!(DurationNs, u64);
index!(LocalUnixNs, i64);

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            pub fn new(bytes: [u8; 16]) -> Result<Self> {
                if bytes.iter().all(|b| *b == 0) {
                    return Err(IdentityError::ZeroIdentifier(stringify!($name)));
                }
                Ok(Self(bytes))
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }
    };
}

opaque_id!(ArchiveId);
opaque_id!(CaptureSessionId);

macro_rules! epoch_advance {
    ($name:ident) => {
        impl $name {
            /// The caller must additionally match this epoch's owner identity.
            pub fn advance(self, expected: Self, next: Self) -> Result<Self> {
                if self != expected {
                    return Err(IdentityError::EpochMismatch);
                }
                if next <= self {
                    return Err(IdentityError::EpochRollback);
                }
                Ok(next)
            }
        }
    };
}

epoch_advance!(ConnectionEpoch);
epoch_advance!(SubscriptionEpoch);
epoch_advance!(BookEpoch);

macro_rules! tagged_enum {
    ($name:ident, $field:literal, $($variant:ident = $tag:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(u8)]
        pub enum $name {
            $($variant = $tag),+
        }

        impl $name {
            pub const fn tag(self) -> u8 {
                self as u8
            }
        }

        impl TryFrom<u8> for $name {
            type Error = IdentityError;

            fn try_from(value: u8) -> Result<Self> {
                match value {
                    $($tag => Ok(Self::$variant),)+
                    _ => Err(IdentityError::UnsupportedTag { field: $field, value }),
                }
            }
        }
    };
}

tagged_enum!(
    MarketKind,
    "MarketKind",
    Spot = 1,
    Perpetual = 2,
    DatedFuture = 3
);
tagged_enum!(BookClass, "BookClass", Normal = 1, Rpi = 2);
tagged_enum!(Channel, "Channel", BookNormal = 1, BookRpi = 2, Trades = 3);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstrumentRef {
    pub venue: Token<32>,
    pub market: MarketKind,
    pub product_namespace: Token<32>,
    pub native_symbol: Token<64>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SpecRef {
    pub instrument: InstrumentRef,
    pub version: SpecVersion,
}

impl SpecRef {
    pub fn ensure_same(&self, other: &Self) -> Result<()> {
        if self.instrument != other.instrument {
            return Err(IdentityError::IdentityMismatch);
        }
        if self.version != other.version {
            return Err(IdentityError::SpecMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BookRef {
    pub instrument: InstrumentRef,
    pub class: BookClass,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EpochTag {
    pub spec: SpecVersion,
    pub connection: ConnectionEpoch,
    pub subscription: SubscriptionEpoch,
    pub book: Option<BookEpoch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamBinding {
    pub id: StreamId,
    pub instrument_slot: InstrumentSlot,
    pub spec: SpecRef,
    pub connection_id: ConnectionId,
    pub channel: Channel,
    pub book_id: Option<BookId>,
    pub tag: EpochTag,
    pub feed_profile: FeedProfileVersion,
}

impl StreamBinding {
    pub fn validate(&self) -> Result<()> {
        if self.tag.spec != self.spec.version {
            return Err(IdentityError::SpecMismatch);
        }
        let is_book = self.channel != Channel::Trades;
        if self.book_id.is_some() != is_book || self.tag.book.is_some() != is_book {
            return Err(IdentityError::InvalidChannelBinding);
        }
        Ok(())
    }

    pub fn book_ref(&self) -> Option<BookRef> {
        let class = match self.channel {
            Channel::BookNormal => BookClass::Normal,
            Channel::BookRpi => BookClass::Rpi,
            Channel::Trades => return None,
        };
        Some(BookRef {
            instrument: self.spec.instrument.clone(),
            class,
        })
    }

    /// Read-only registration guard; callers commit only after all checks pass.
    pub fn validate_registration(&self, previous: &[Self]) -> Result<()> {
        self.validate()?;
        for old in previous {
            if old.id == self.id {
                return Err(IdentityError::IdentityConflict);
            }
            if self.book_ref().is_some() && old.book_ref() == self.book_ref() {
                return Err(IdentityError::WriterRebindRequiresNewArchive);
            }
            if self.book_id.is_some() && old.book_id == self.book_id {
                return Err(IdentityError::IdentityConflict);
            }
            if old.connection_id == self.connection_id && old.tag.connection != self.tag.connection
            {
                return Err(IdentityError::EpochMismatch);
            }
            if (old.instrument_slot == self.instrument_slot)
                != (old.spec.instrument == self.spec.instrument)
            {
                return Err(IdentityError::IdentityConflict);
            }
        }
        Ok(())
    }
}
