//! The numerical subset of InstrumentSpec, without a loader or source proof.
//! Reference/units are checked before parsing or converting any scalar.

use std::cmp::Ordering;
use std::fmt;

use crate::identity::{IdentityError, SpecRef, Token};
use crate::numeric::{ExactDecimal, NumericError, PriceTicks, QuantitySteps};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueError {
    Identity(IdentityError),
    UnitMismatch,
    Numeric(NumericError),
}

impl From<NumericError> for ValueError {
    fn from(error: NumericError) -> Self {
        Self::Numeric(error)
    }
}

impl From<IdentityError> for ValueError {
    fn from(error: IdentityError) -> Self {
        Self::Identity(error)
    }
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ValueError {}

type Result<T> = std::result::Result<T, ValueError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceUnits {
    pub quote: Token<32>,
    pub basis: Token<32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericSpecFields {
    pub reference: SpecRef,
    pub price_units: PriceUnits,
    pub quantity_unit: Token<32>,
    pub base_asset: Token<32>,
    pub price_increment: ExactDecimal,
    pub quantity_increment: ExactDecimal,
    /// A caller-supplied constant linear multiplier, not proof of exchange facts.
    pub quantity_to_base_multiplier: Option<ExactDecimal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericSpec(NumericSpecFields);

impl NumericSpec {
    pub fn new(fields: NumericSpecFields) -> Result<Self> {
        if fields.price_increment.is_zero() || fields.quantity_increment.is_zero() {
            return Err(NumericError::InvalidIncrement.into());
        }
        if fields
            .quantity_to_base_multiplier
            .is_some_and(ExactDecimal::is_zero)
        {
            return Err(NumericError::InvalidMultiplier.into());
        }
        if fields.quantity_unit == fields.base_asset
            && fields.quantity_to_base_multiplier != Some(ExactDecimal::ONE)
        {
            return Err(NumericError::InvalidMultiplier.into());
        }
        Ok(Self(fields))
    }

    pub fn fields(&self) -> &NumericSpecFields {
        &self.0
    }

    pub fn parse_price(
        &self,
        reference: &SpecRef,
        units: &PriceUnits,
        input: &str,
    ) -> Result<PricedValue> {
        self.0.reference.ensure_same(reference)?;
        if self.0.price_units != *units {
            return Err(ValueError::UnitMismatch);
        }
        let ticks = input
            .parse::<ExactDecimal>()?
            .to_price_ticks(self.0.price_increment)?;
        Ok(PricedValue::new(reference.clone(), ticks))
    }

    pub fn parse_quantity(
        &self,
        reference: &SpecRef,
        unit: &Token<32>,
        input: &str,
    ) -> Result<SizedValue> {
        self.0.reference.ensure_same(reference)?;
        if self.0.quantity_unit != *unit {
            return Err(ValueError::UnitMismatch);
        }
        let steps = input
            .parse::<ExactDecimal>()?
            .to_quantity_steps(self.0.quantity_increment)?;
        Ok(SizedValue::new(reference.clone(), steps))
    }

    pub fn price_decimal(&self, value: &PricedValue) -> Result<ExactDecimal> {
        self.0.reference.ensure_same(&value.reference)?;
        Ok(ExactDecimal::from_count(
            value.ticks.get(),
            self.0.price_increment,
        )?)
    }

    pub fn quantity_decimal(&self, value: &SizedValue) -> Result<ExactDecimal> {
        self.0.reference.ensure_same(&value.reference)?;
        Ok(ExactDecimal::from_count(
            value.steps.get(),
            self.0.quantity_increment,
        )?)
    }

    pub fn convert_quantity(
        &self,
        value: &SizedValue,
        request: QuantityConversion<'_>,
    ) -> Result<ExactDecimal> {
        self.0.reference.ensure_same(&value.reference)?;
        let QuantityConversion::LinearBase(unit) = request else {
            return Err(NumericError::UnsupportedConversion.into());
        };
        if self.0.base_asset != *unit {
            return Err(ValueError::UnitMismatch);
        }
        Ok(value.steps.to_base(
            self.0.quantity_increment,
            self.0.quantity_to_base_multiplier,
        )?)
    }

    pub fn base_to_quantity(
        &self,
        reference: &SpecRef,
        base_unit: &Token<32>,
        value: ExactDecimal,
    ) -> Result<SizedValue> {
        self.0.reference.ensure_same(reference)?;
        if self.0.base_asset != *base_unit {
            return Err(ValueError::UnitMismatch);
        }
        let steps = value.base_to_steps(
            self.0.quantity_increment,
            self.0.quantity_to_base_multiplier,
        )?;
        Ok(SizedValue::new(reference.clone(), steps))
    }
}

/// A nonlinear valuation request is rejected, not approximated as a multiplier.
#[derive(Clone, Copy, Debug)]
pub enum QuantityConversion<'a> {
    LinearBase(&'a Token<32>),
    NonlinearValuation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PricedValue {
    reference: SpecRef,
    ticks: PriceTicks,
}

impl PricedValue {
    pub fn new(reference: SpecRef, ticks: PriceTicks) -> Self {
        Self { reference, ticks }
    }

    pub fn reference(&self) -> &SpecRef {
        &self.reference
    }

    pub const fn ticks(&self) -> PriceTicks {
        self.ticks
    }

    pub fn checked_cmp(&self, other: &Self) -> Result<Ordering> {
        self.reference.ensure_same(&other.reference)?;
        Ok(self.ticks.cmp(&other.ticks))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SizedValue {
    reference: SpecRef,
    steps: QuantitySteps,
}

impl SizedValue {
    pub fn new(reference: SpecRef, steps: QuantitySteps) -> Self {
        Self { reference, steps }
    }

    pub fn reference(&self) -> &SpecRef {
        &self.reference
    }

    pub const fn steps(&self) -> QuantitySteps {
        self.steps
    }

    pub fn checked_cmp(&self, other: &Self) -> Result<Ordering> {
        self.reference.ensure_same(&other.reference)?;
        Ok(self.steps.cmp(&other.steps))
    }
}
