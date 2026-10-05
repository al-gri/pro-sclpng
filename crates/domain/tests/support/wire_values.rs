//! Domain value encodings shared by test-local WAL and artifact helpers.

use domain::artifact::{ArtifactError, ArtifactRef};
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{PolicyFields, RecordingGate, SilenceRule};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::WireContext;

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked};

macro_rules! read_id {
    ($name:ident, $ty:ident, $read:ident) => {
        pub fn $name(&mut self) -> Result<$ty> {
            let offset = self.offset();
            let value = self.$read()?;
            checked(offset, $ty::new(value))
        }
    };
}

impl Reader<'_> {
    read_id!(spec_version, SpecVersion, u32);
    read_id!(config_version, ConfigVersion, u32);
    read_id!(normalizer_version, NormalizerVersion, u32);
    read_id!(profile_version, FeedProfileVersion, u32);
    read_id!(slot, InstrumentSlot, u32);
    read_id!(stream, StreamId, u32);
    read_id!(connection, ConnectionId, u32);
    read_id!(book, BookId, u32);
    read_id!(clock, ClockId, u32);
    read_id!(connection_epoch, ConnectionEpoch, u64);
    read_id!(subscription_epoch, SubscriptionEpoch, u64);
    read_id!(book_epoch, BookEpoch, u64);
    read_id!(record_no, RecordNo, u64);
    read_id!(attempt, CaptureAttemptNo, u64);

    pub fn enum_tag<T: TryFrom<u8>>(&mut self, field: &'static str) -> Result<T> {
        let offset = self.offset();
        let value = self.u8()?;
        T::try_from(value).map_err(|_| {
            Error::new(offset, ErrorKind::Unsupported { field, value: value.into() })
        })
    }

    pub fn archive(&mut self) -> Result<ArchiveId> {
        let offset = self.offset();
        checked(offset, ArchiveId::new(self.array()?))
    }

    pub fn session(&mut self) -> Result<CaptureSessionId> {
        let offset = self.offset();
        checked(offset, CaptureSessionId::new(self.array()?))
    }

    pub fn token<const N: usize>(&mut self) -> Result<Token<N>> {
        let offset = self.offset();
        let length = usize::from(self.u8()?);
        if length == 0 || length > N || N > 128 {
            return Err(Error::new(offset, ErrorKind::Identity(IdentityError::InvalidToken)));
        }
        let text = std::str::from_utf8(self.take(length)?).map_err(|_| {
            Error::new(offset, ErrorKind::Identity(IdentityError::InvalidToken))
        })?;
        checked(offset, Token::new(text))
    }

    pub fn artifact(&mut self) -> Result<ArtifactRef> {
        let offset = self.offset();
        if self.u8()? != 71 {
            return Err(Error::new(offset, ErrorKind::Artifact(ArtifactError::InvalidArtifactRef)));
        }
        let text = std::str::from_utf8(self.take(71)?).map_err(|_| {
            Error::new(offset, ErrorKind::Artifact(ArtifactError::InvalidArtifactRef))
        })?;
        checked(offset, text.parse())
    }

    pub fn decimal(&mut self) -> Result<ExactDecimal> {
        let offset = self.offset();
        let coefficient = self.u128()?;
        let scale = self.u8()?;
        checked(offset, ExactDecimal::from_canonical(coefficient, scale))
    }

    pub fn instrument(&mut self) -> Result<InstrumentRef> {
        Ok(InstrumentRef {
            venue: self.token()?,
            market: self.enum_tag("MarketKind")?,
            product_namespace: self.token()?,
            native_symbol: self.token()?,
        })
    }

    pub fn epoch_tag(&mut self) -> Result<EpochTag> {
        Ok(EpochTag {
            spec: self.spec_version()?,
            connection: self.connection_epoch()?,
            subscription: self.subscription_epoch()?,
            book: self.option(Self::book_epoch)?,
        })
    }

    pub fn context(&mut self, kind: u16, has_active: bool) -> Result<WireContext> {
        let unix_ns = LocalUnixNs::new(self.i64()?);
        let monotonic_ns = MonotonicNs::new(self.u64()?);
        let offset = self.offset();
        let config = self.u32()?;
        let normalizer = self.u32()?;
        let context = checked(offset, InputContext::decode(config, normalizer, kind, has_active))?;
        Ok(WireContext { unix_ns, monotonic_ns, context })
    }

    pub fn active_context(&mut self) -> Result<ActiveContext> {
        Ok(ActiveContext {
            config: self.config_version()?,
            normalizer: self.normalizer_version()?,
        })
    }

    pub fn policy_fields(&mut self) -> Result<PolicyFields> {
        Ok(PolicyFields {
            silence_rule: self.enum_tag::<SilenceRule>("Config.silence_rule")?,
            freshness_deadline_ns: self.option(Self::u64)?,
            warmup_min_updates: self.option(Self::u32)?,
            warmup_min_elapsed_ns: self.option(Self::u64)?,
            allow_quiet_with_proof: self.bool()?,
            require_two_sided_snapshot: self.bool()?,
            recording_gate: self.enum_tag::<RecordingGate>("Config.recording_gate")?,
        })
    }

    /// The InstrumentSpec body after Context and before its provenance ref.
    pub fn numeric_spec(&mut self) -> Result<(InstrumentSlot, NumericSpec)> {
        let offset = self.offset();
        let slot = self.slot()?;
        let version = self.spec_version()?;
        let instrument = self.instrument()?;
        let fields = NumericSpecFields {
            reference: SpecRef { instrument, version },
            price_units: PriceUnits { quote: self.token()?, basis: self.token()? },
            quantity_unit: self.token()?,
            base_asset: self.token()?,
            price_increment: self.decimal()?,
            quantity_increment: self.decimal()?,
            quantity_to_base_multiplier: self.option(Self::decimal)?,
        };
        Ok((slot, checked(offset, NumericSpec::new(fields))?))
    }
}

impl Writer {
    pub fn token<const N: usize>(&mut self, value: &Token<N>) -> Result<()> {
        let length = u8::try_from(value.as_str().len())
            .map_err(|_| Error::new(0, ErrorKind::LengthError))?;
        self.u8(length)?;
        self.bytes(value.as_str().as_bytes())
    }

    pub fn artifact(&mut self, value: ArtifactRef) -> Result<()> {
        self.u8(71)?;
        self.bytes(value.to_string().as_bytes())
    }

    pub fn decimal(&mut self, value: ExactDecimal) -> Result<()> {
        self.u128(value.coefficient())?;
        self.u8(value.scale())
    }

    pub fn instrument(&mut self, value: &InstrumentRef) -> Result<()> {
        self.token(&value.venue)?;
        self.u8(value.market.tag())?;
        self.token(&value.product_namespace)?;
        self.token(&value.native_symbol)
    }

    pub fn epoch_tag(&mut self, value: EpochTag) -> Result<()> {
        self.u32(value.spec.get())?;
        self.u64(value.connection.get())?;
        self.u64(value.subscription.get())?;
        self.option(value.book, |w, epoch| w.u64(epoch.get()))
    }

    pub fn active_context(&mut self, value: ActiveContext) -> Result<()> {
        self.u32(value.config.get())?;
        self.u32(value.normalizer.get())
    }

    pub fn context(&mut self, value: WireContext) -> Result<()> {
        self.i64(value.unix_ns.get())?;
        self.u64(value.monotonic_ns.get())?;
        match value.context {
            InputContext::Bootstrap => {
                self.u32(0)?;
                self.u32(0)
            }
            InputContext::Active(active) => self.active_context(active),
        }
    }

    pub fn policy_fields(&mut self, value: PolicyFields) -> Result<()> {
        self.u8(value.silence_rule.tag())?;
        self.option(value.freshness_deadline_ns, Self::u64)?;
        self.option(value.warmup_min_updates, Self::u32)?;
        self.option(value.warmup_min_elapsed_ns, Self::u64)?;
        self.bool(value.allow_quiet_with_proof)?;
        self.bool(value.require_two_sided_snapshot)?;
        self.u8(value.recording_gate.tag())
    }

    pub fn numeric_spec(&mut self, slot: InstrumentSlot, value: &NumericSpec) -> Result<()> {
        let fields = value.fields();
        self.u32(slot.get())?;
        self.u32(fields.reference.version.get())?;
        self.instrument(&fields.reference.instrument)?;
        self.token(&fields.price_units.quote)?;
        self.token(&fields.price_units.basis)?;
        self.token(&fields.quantity_unit)?;
        self.token(&fields.base_asset)?;
        self.decimal(fields.price_increment)?;
        self.decimal(fields.quantity_increment)?;
        self.option(fields.quantity_to_base_multiplier, Self::decimal)
    }
}
