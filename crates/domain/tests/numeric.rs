//! SPEC-001: origin=synthetic, schema=1, proposal=2, policy=not_applicable.
//! Expected values below come from N01-N20, not from another production codec.

use domain::numeric::{ExactDecimal, NumericError as E, Operation as O, PriceTicks, QuantitySteps};

fn d(text: &str) -> ExactDecimal {
    text.parse().unwrap()
}

#[test]
fn n01_n03_price_grid_and_canonical_reverse() {
    let tick = d("0.0500");
    let value = d("000100.1000");
    assert_eq!((tick.coefficient(), tick.scale()), (5, 2));
    assert_eq!((value.coefficient(), value.scale()), (1001, 1));
    assert_eq!(value.to_price_ticks(tick).unwrap().get(), 2002);
    assert_eq!(
        ExactDecimal::from_count(2002, tick).unwrap().to_string(),
        "100.1"
    );
}

#[test]
fn n02_quantity_grid_and_reverse() {
    assert_eq!(
        d("1.234").to_quantity_steps(d("0.001")).unwrap().get(),
        1234
    );
    assert_eq!(
        ExactDecimal::from_count(1234, d("0.001")).unwrap(),
        d("1.234")
    );
}

#[test]
fn n04_n05_off_grid_never_rounds() {
    assert_eq!(d("100.11").to_price_ticks(d("0.05")), Err(E::OffGrid));
    assert_eq!(d("1.2345").to_quantity_steps(d("0.001")), Err(E::OffGrid));
}

#[test]
fn n06_invalid_ascii_grammar() {
    for input in [
        "", ".5", "1.", "1e2", "NaN", "inf", "1,2", " 1", "1\t", "1\n", "١", "１", "1.2.3",
    ] {
        assert_eq!(
            input.parse::<ExactDecimal>(),
            Err(E::InvalidSyntax),
            "{input:?}"
        );
    }
}

#[test]
fn n07_signs_are_rejected() {
    for input in ["+1", "-1", "-0"] {
        assert_eq!(input.parse::<ExactDecimal>(), Err(E::InvalidSign));
    }
}

#[test]
fn n08_input_bound_and_error_priority() {
    assert_eq!(
        "0".repeat(96).parse::<ExactDecimal>(),
        Ok(ExactDecimal::ZERO)
    );
    assert_eq!("0".repeat(97).parse::<ExactDecimal>(), Err(E::InputTooLong));
    assert_eq!(
        format!("-{}", "0".repeat(96)).parse::<ExactDecimal>(),
        Err(E::InputTooLong)
    );
    assert_eq!(ExactDecimal::ZERO.to_price_ticks(d("1")), Err(E::ZeroPrice));
}

#[test]
fn n09_significant_scale_only() {
    assert_eq!(d("1.0000000000000000000"), ExactDecimal::ONE);
    assert_eq!(
        "0.0000000000000000001".parse::<ExactDecimal>(),
        Err(E::ScaleTooLarge)
    );
    assert_eq!(
        d("0.000000000000000001").to_string(),
        "0.000000000000000001"
    );
}

#[test]
fn n10_n11_count_boundaries() {
    for count in [1, u64::MAX] {
        let value = d(&count.to_string());
        assert_eq!(value.to_price_ticks(d("1")).unwrap().get(), count);
        assert_eq!(ExactDecimal::from_count(count, d("1")), Ok(value));
    }
    assert_eq!(
        d("18446744073709551616").to_price_ticks(d("1")),
        Err(E::CountOutOfRange)
    );
    assert_eq!(PriceTicks::new(0), Err(E::ZeroPrice));
    assert_eq!(PriceTicks::new(u64::MAX).unwrap().get(), u64::MAX);
}

#[test]
fn n12_coefficient_max_and_add_overflow() {
    assert_eq!(d(&u128::MAX.to_string()).coefficient(), u128::MAX);
    assert_eq!(
        "340282366920938463463374607431768211456".parse::<ExactDecimal>(),
        Err(E::Overflow(O::CoefficientAdd))
    );
}

#[test]
fn n13_coefficient_multiply_overflow() {
    assert_eq!(
        format!("{}0", u128::MAX).parse::<ExactDecimal>(),
        Err(E::Overflow(O::CoefficientMultiply))
    );
}

#[test]
fn n14_metadata_does_not_repair_noncanonical_values() {
    assert_eq!(ExactDecimal::from_canonical(1, 19), Err(E::ScaleTooLarge));
    assert_eq!(
        ExactDecimal::from_canonical(100, 2),
        Err(E::NonCanonicalDecimal)
    );
    assert_eq!(
        ExactDecimal::from_canonical(0, 1),
        Err(E::NonCanonicalDecimal)
    );
    assert_eq!(
        d("1").to_quantity_steps(ExactDecimal::ZERO),
        Err(E::InvalidIncrement)
    );
    assert_eq!(
        ExactDecimal::from_count(0, ExactDecimal::ZERO),
        Err(E::InvalidIncrement)
    );
}

#[test]
fn n15_alignment_overflow_is_not_cancelled() {
    assert_eq!(
        d(&u128::MAX.to_string()).to_quantity_steps(d("0.000000000000000001")),
        Err(E::Overflow(O::AlignValue))
    );
    assert_eq!(
        d("0.1").to_quantity_steps(d(&u128::MAX.to_string())),
        Err(E::Overflow(O::AlignIncrement))
    );
}

#[test]
fn n16_reverse_multiply_overflow() {
    assert_eq!(
        ExactDecimal::from_count(u64::MAX, d(&u128::MAX.to_string())),
        Err(E::Overflow(O::ReverseMultiply))
    );
}

#[test]
fn n17_constant_multiplier_round_trip() {
    let base = QuantitySteps::new(123)
        .to_base(d("1"), Some(d("0.01")))
        .unwrap();
    assert_eq!(base, d("1.23"));
    assert_eq!(
        base.base_to_steps(d("1"), Some(d("0.01"))),
        Ok(QuantitySteps::new(123))
    );
    assert_eq!(
        d("1.231").base_to_steps(d("1"), Some(d("0.01"))),
        Err(E::OffGrid)
    );
}

#[test]
fn n18_unknown_and_invalid_multiplier() {
    assert_eq!(
        QuantitySteps::new(1).to_base(d("1"), None),
        Err(E::UnknownMultiplier)
    );
    assert_eq!(
        QuantitySteps::new(1).to_base(d("1"), Some(d("0"))),
        Err(E::InvalidMultiplier)
    );
    assert_eq!(
        d("1").base_to_steps(d("1"), None),
        Err(E::UnknownMultiplier)
    );
    // Independent grid conversion must not need a multiplier.
    assert_eq!(d("2").to_quantity_steps(d("1")), Ok(QuantitySteps::new(2)));
}

#[test]
fn n19_intermediate_overflow_and_final_scale() {
    let max = d(&u128::MAX.to_string());
    assert_eq!(
        QuantitySteps::new(2).to_base(max, Some(d("1"))),
        Err(E::Overflow(O::QuantityMultiply))
    );
    assert_eq!(
        QuantitySteps::new(1).to_base(max, Some(d("2"))),
        Err(E::Overflow(O::MultiplierMultiply))
    );
    let tiny = d("0.000000000000000001");
    assert_eq!(
        QuantitySteps::new(1).to_base(tiny, Some(tiny)),
        Err(E::ScaleTooLarge)
    );
    let count = 1_000_000_000_000_000_000;
    assert_eq!(
        QuantitySteps::new(count).to_base(tiny, Some(tiny)),
        Ok(tiny)
    );
    assert_eq!(
        tiny.base_to_steps(tiny, Some(tiny)),
        Ok(QuantitySteps::new(count))
    );
}

#[test]
fn n20_numerical_zero_is_not_trade_or_level() {
    let zero = QuantitySteps::new(0);
    assert_eq!(zero.get(), 0);
    assert_eq!(zero.for_trade(), Err(E::ZeroTradeQuantity));
    assert_eq!(zero.for_level(), Err(E::ZeroLevelQuantity));
    assert_eq!(zero.to_base(d("1"), Some(d("1"))), Ok(ExactDecimal::ZERO));
    assert_eq!(ExactDecimal::ZERO.to_string(), "0");
}

fn scaled(coefficient: u128, scale: u32) -> ExactDecimal {
    if scale == 0 {
        return d(&coefficient.to_string());
    }
    let power = 10_u128.pow(scale);
    d(&format!(
        "{}.{:0width$}",
        coefficient / power,
        coefficient % power,
        width = scale as usize
    ))
}

#[test]
fn n_exhaustive_small_grids_and_reverse() {
    for coefficient in 0..=200_u128 {
        for scale in 0..=3_u32 {
            let value = scaled(coefficient, scale);
            for increment in 1..=20_u128 {
                for increment_scale in 0..=3_u32 {
                    let grid = scaled(increment, increment_scale);
                    let common = scale.max(increment_scale);
                    let x = coefficient * 10_u128.pow(common - scale);
                    let step = increment * 10_u128.pow(common - increment_scale);
                    let actual = value.to_quantity_steps(grid);
                    if x.is_multiple_of(step) {
                        let count = u64::try_from(x / step).unwrap();
                        assert_eq!(actual, Ok(QuantitySteps::new(count)));
                        assert_eq!(ExactDecimal::from_count(count, grid), Ok(value));
                    } else {
                        assert_eq!(actual, Err(E::OffGrid));
                    }
                }
            }
        }
    }
    for increment in 1..=20_u128 {
        for scale in 0..=3_u32 {
            let grid = scaled(increment, scale);
            for count in 0..=100_u64 {
                let value = ExactDecimal::from_count(count, grid).unwrap();
                assert_eq!(value.to_quantity_steps(grid), Ok(QuantitySteps::new(count)));
            }
        }
    }
}
