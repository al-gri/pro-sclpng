//! Bounded exact arithmetic from types-v1, D1/D2.
//!
//! These scalar operations do not establish instrument identity or source units.
//! Qualified conversions must check metadata before invoking them. No floats,
//! rounding, saturation, arbitrary precision or implicit multiplier are used.

use std::fmt;
use std::str::FromStr;

pub const MAX_DECIMAL_INPUT_BYTES: usize = 96;
pub const MAX_DECIMAL_SCALE: u8 = 18;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    CoefficientMultiply,
    CoefficientAdd,
    AlignValue,
    AlignIncrement,
    ReverseMultiply,
    QuantityMultiply,
    MultiplierMultiply,
    ScaleAdd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericError {
    InputTooLong,
    InvalidSign,
    InvalidSyntax,
    ScaleTooLarge,
    NonCanonicalDecimal,
    InvalidIncrement,
    InvalidMultiplier,
    UnknownMultiplier,
    UnsupportedConversion,
    Overflow(Operation),
    CountOutOfRange,
    OffGrid,
    ZeroPrice,
    ZeroTradeQuantity,
    ZeroLevelQuantity,
}

impl fmt::Display for NumericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for NumericError {}

type Result<T> = std::result::Result<T, NumericError>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExactDecimal {
    coefficient: u128,
    scale: u8,
}

impl ExactDecimal {
    pub const ZERO: Self = Self {
        coefficient: 0,
        scale: 0,
    };
    pub const ONE: Self = Self {
        coefficient: 1,
        scale: 0,
    };

    /// Decode already-canonical metadata. Unlike parsing, this never repairs it.
    pub fn from_canonical(coefficient: u128, scale: u8) -> Result<Self> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(NumericError::ScaleTooLarge);
        }
        if (coefficient == 0 && scale != 0) || (scale > 0 && coefficient.is_multiple_of(10)) {
            return Err(NumericError::NonCanonicalDecimal);
        }
        Ok(Self { coefficient, scale })
    }

    fn normalize(mut coefficient: u128, mut scale: u8) -> Result<Self> {
        if coefficient == 0 {
            return Ok(Self::ZERO);
        }
        while scale > 0 && coefficient.is_multiple_of(10) {
            coefficient /= 10;
            scale -= 1;
        }
        Self::from_canonical(coefficient, scale)
    }

    pub const fn coefficient(self) -> u128 {
        self.coefficient
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub const fn is_zero(self) -> bool {
        self.coefficient == 0
    }

    pub fn to_price_ticks(self, increment: Self) -> Result<PriceTicks> {
        PriceTicks::new(self.grid_count(increment.coefficient, increment.scale)?)
    }

    pub fn to_quantity_steps(self, increment: Self) -> Result<QuantitySteps> {
        Ok(QuantitySteps::new(
            self.grid_count(increment.coefficient, increment.scale)?,
        ))
    }

    fn grid_count(self, coefficient: u128, scale: u8) -> Result<u64> {
        if coefficient == 0 {
            return Err(NumericError::InvalidIncrement);
        }
        let common = self.scale.max(scale);
        let x = align(self.coefficient, common - self.scale, Operation::AlignValue)?;
        let d = align(coefficient, common - scale, Operation::AlignIncrement)?;
        if !x.is_multiple_of(d) {
            return Err(NumericError::OffGrid);
        }
        u64::try_from(x / d).map_err(|_| NumericError::CountOutOfRange)
    }

    pub fn from_count(count: u64, increment: Self) -> Result<Self> {
        if increment.is_zero() {
            return Err(NumericError::InvalidIncrement);
        }
        let coefficient = multiply(
            u128::from(count),
            increment.coefficient,
            Operation::ReverseMultiply,
        )?;
        Self::normalize(coefficient, increment.scale)
    }

    /// Exact inverse of a constant linear quantity-to-base conversion.
    /// The internal denominator may have scale 36; it is not prematurely
    /// narrowed to an external ExactDecimal with scale <= 18.
    pub fn base_to_steps(self, step: Self, multiplier: Option<Self>) -> Result<QuantitySteps> {
        let multiplier = checked_multiplier(step, multiplier)?;
        let mut coefficient = multiply(
            step.coefficient,
            multiplier.coefficient,
            Operation::MultiplierMultiply,
        )?;
        let mut scale = step
            .scale
            .checked_add(multiplier.scale)
            .ok_or(NumericError::Overflow(Operation::ScaleAdd))?;
        while scale > 0 && coefficient.is_multiple_of(10) {
            coefficient /= 10;
            scale -= 1;
        }
        Ok(QuantitySteps::new(self.grid_count(coefficient, scale)?))
    }
}

impl FromStr for ExactDecimal {
    type Err = NumericError;

    fn from_str(input: &str) -> Result<Self> {
        if input.len() > MAX_DECIMAL_INPUT_BYTES {
            return Err(NumericError::InputTooLong);
        }
        if matches!(input.as_bytes().first(), Some(b'+' | b'-')) {
            return Err(NumericError::InvalidSign);
        }
        let (integer, fraction) = match input.split_once('.') {
            Some((integer, fraction)) => {
                if fraction.is_empty() {
                    return Err(NumericError::InvalidSyntax);
                }
                (integer, fraction)
            }
            None => (input, ""),
        };
        if integer.is_empty()
            || !integer.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(NumericError::InvalidSyntax);
        }
        let integer = integer.trim_start_matches('0');
        let fraction = fraction.trim_end_matches('0');
        if fraction.len() > usize::from(MAX_DECIMAL_SCALE) {
            return Err(NumericError::ScaleTooLarge);
        }
        let mut coefficient = 0_u128;
        for digit in integer.bytes().chain(fraction.bytes()) {
            coefficient = multiply(coefficient, 10, Operation::CoefficientMultiply)?;
            coefficient = coefficient
                .checked_add(u128::from(digit - b'0'))
                .ok_or(NumericError::Overflow(Operation::CoefficientAdd))?;
        }
        let scale = u8::try_from(fraction.len()).map_err(|_| NumericError::ScaleTooLarge)?;
        Self::normalize(coefficient, scale)
    }
}

impl fmt::Display for ExactDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.coefficient.to_string();
        let scale = usize::from(self.scale);
        if scale == 0 {
            return f.write_str(&digits);
        }
        if digits.len() > scale {
            let split = digits.len() - scale;
            return write!(f, "{}.{}", &digits[..split], &digits[split..]);
        }
        f.write_str("0.")?;
        for _ in 0..scale - digits.len() {
            f.write_str("0")?;
        }
        f.write_str(&digits)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PriceTicks(u64);

impl PriceTicks {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(NumericError::ZeroPrice);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QuantitySteps(u64);

impl QuantitySteps {
    /// Every u64, including zero, is a valid numerical step count.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn for_trade(self) -> Result<Self> {
        if self.0 == 0 {
            return Err(NumericError::ZeroTradeQuantity);
        }
        Ok(self)
    }

    pub fn for_level(self) -> Result<Self> {
        if self.0 == 0 {
            return Err(NumericError::ZeroLevelQuantity);
        }
        Ok(self)
    }

    pub fn to_base(
        self,
        step: ExactDecimal,
        multiplier: Option<ExactDecimal>,
    ) -> Result<ExactDecimal> {
        let multiplier = checked_multiplier(step, multiplier)?;
        let a = multiply(
            u128::from(self.0),
            step.coefficient,
            Operation::QuantityMultiply,
        )?;
        let b = multiply(a, multiplier.coefficient, Operation::MultiplierMultiply)?;
        let scale = step
            .scale
            .checked_add(multiplier.scale)
            .ok_or(NumericError::Overflow(Operation::ScaleAdd))?;
        ExactDecimal::normalize(b, scale)
    }
}

fn multiply(a: u128, b: u128, operation: Operation) -> Result<u128> {
    a.checked_mul(b).ok_or(NumericError::Overflow(operation))
}

fn align(coefficient: u128, exponent: u8, operation: Operation) -> Result<u128> {
    let factor = 10_u128
        .checked_pow(u32::from(exponent))
        .ok_or(NumericError::Overflow(operation))?;
    multiply(coefficient, factor, operation)
}

fn checked_multiplier(
    step: ExactDecimal,
    multiplier: Option<ExactDecimal>,
) -> Result<ExactDecimal> {
    if step.is_zero() {
        return Err(NumericError::InvalidIncrement);
    }
    let multiplier = multiplier.ok_or(NumericError::UnknownMultiplier)?;
    if multiplier.is_zero() {
        return Err(NumericError::InvalidMultiplier);
    }
    Ok(multiplier)
}
