//! origin=synthetic; schema=1; proposal=2; policy=not_applicable.

use domain::identity::IdentityError as E;
use domain::identity::*;
use domain::numeric::{ExactDecimal, NumericError as N, PriceTicks, QuantitySteps};
use domain::qualified::ValueError as V;
use domain::qualified::*;
use std::slice::from_ref;

fn instrument(market: MarketKind) -> InstrumentRef {
    InstrumentRef {
        venue: Token::new("SYN").unwrap(),
        market,
        product_namespace: Token::new("product").unwrap(),
        native_symbol: Token::new("ABCUSD").unwrap(),
    }
}

fn reference() -> SpecRef {
    SpecRef {
        instrument: instrument(MarketKind::Spot),
        version: SpecVersion::new(1).unwrap(),
    }
}

fn fields() -> NumericSpecFields {
    NumericSpecFields {
        reference: reference(),
        price_units: PriceUnits {
            quote: Token::new("USD").unwrap(),
            basis: Token::new("ABC").unwrap(),
        },
        quantity_unit: Token::new("contract").unwrap(),
        base_asset: Token::new("ABC").unwrap(),
        price_increment: "0.05".parse().unwrap(),
        quantity_increment: "1".parse().unwrap(),
        quantity_to_base_multiplier: Some("0.01".parse().unwrap()),
    }
}

fn stream(id: u32, channel: Channel) -> StreamBinding {
    let is_book = channel != Channel::Trades;
    StreamBinding {
        id: StreamId::new(id).unwrap(),
        instrument_slot: InstrumentSlot::new(1).unwrap(),
        spec: reference(),
        connection_id: ConnectionId::new(1).unwrap(),
        channel,
        book_id: is_book.then(|| BookId::new(id).unwrap()),
        tag: EpochTag {
            spec: SpecVersion::new(1).unwrap(),
            connection: ConnectionEpoch::new(1).unwrap(),
            subscription: SubscriptionEpoch::new(1).unwrap(),
            book: is_book.then(|| BookEpoch::new(1).unwrap()),
        },
        feed_profile: FeedProfileVersion::new(1).unwrap(),
    }
}

#[test]
fn identity_tokens_are_bounded_ascii_without_case_folding() {
    for text in ["", " ", "a b", "a\0", "аб", "a+", "a\\b"] {
        assert_eq!(Token::<32>::new(text), Err(E::InvalidToken));
    }
    assert_eq!(
        Token::<32>::new(&"a".repeat(32)).unwrap().as_str().len(),
        32
    );
    assert_eq!(Token::<32>::new(&"a".repeat(33)), Err(E::InvalidToken));
    assert_eq!(Token::<0>::new("a"), Err(E::InvalidToken));
    assert_eq!(Token::<129>::new("a"), Err(E::InvalidToken));
    let upper = Token::<32>::new("ABC").unwrap();
    let lower = Token::<32>::new("abc").unwrap();
    assert_ne!(upper, lower);
    assert_eq!(Token::<32>::new("a._:/-9").unwrap().as_str(), "a._:/-9");
}

#[test]
fn e01_e02_market_and_book_identities_are_distinct() {
    let spot = instrument(MarketKind::Spot);
    let perpetual = instrument(MarketKind::Perpetual);
    assert_ne!(spot, perpetual);
    let normal = stream(1, Channel::BookNormal);
    let rpi = stream(2, Channel::BookRpi);
    assert_ne!(normal.book_ref(), rpi.book_ref());
    assert_eq!(normal.tag.book, rpi.tag.book);
    assert_eq!(rpi.validate_registration(&[normal]), Ok(()));
    assert_eq!(
        Channel::try_from(0),
        Err(E::UnsupportedTag {
            field: "Channel",
            value: 0
        })
    );
    assert_eq!(
        MarketKind::try_from(255),
        Err(E::UnsupportedTag {
            field: "MarketKind",
            value: 255
        })
    );
}

#[test]
fn e03_channel_scope_and_slot_conflicts() {
    let mut bad = stream(1, Channel::Trades);
    bad.book_id = Some(BookId::new(1).unwrap());
    assert_eq!(bad.validate(), Err(E::InvalidChannelBinding));
    let normal = stream(1, Channel::BookNormal);
    let mut other = stream(2, Channel::Trades);
    other.instrument_slot = InstrumentSlot::new(2).unwrap();
    assert_eq!(
        other.validate_registration(from_ref(&normal)),
        Err(E::IdentityConflict)
    );
    other.instrument_slot = normal.instrument_slot;
    other.spec.instrument = instrument(MarketKind::Perpetual);
    assert_eq!(
        other.validate_registration(&[normal]),
        Err(E::IdentityConflict)
    );
}

#[test]
fn v_c1_writer_and_shared_connection_guards() {
    let normal = stream(1, Channel::BookNormal);
    let second_writer = stream(2, Channel::BookNormal);
    assert_eq!(
        second_writer.validate_registration(from_ref(&normal)),
        Err(E::WriterRebindRequiresNewArchive)
    );
    let mut rpi = stream(2, Channel::BookRpi);
    assert_eq!(rpi.validate_registration(from_ref(&normal)), Ok(()));
    rpi.tag.connection = ConnectionEpoch::new(2).unwrap();
    assert_eq!(
        rpi.validate_registration(from_ref(&normal)),
        Err(E::EpochMismatch)
    );
    assert_eq!(
        normal.validate_registration(from_ref(&normal)),
        Err(E::IdentityConflict)
    );
}

#[test]
fn e16_positive_ids_and_checked_exhaustion() {
    macro_rules! check {
        ($name:ident, $integer:ty) => {
            assert_eq!($name::new(0), Err(E::ZeroIdentifier(stringify!($name))));
            assert_eq!($name::new(1).unwrap().checked_next().unwrap().get(), 2);
            assert_eq!(
                $name::new(<$integer>::MAX).unwrap().checked_next(),
                Err(E::CounterExhausted(stringify!($name)))
            );
        };
    }
    check!(SpecVersion, u32);
    check!(ConfigVersion, u32);
    check!(NormalizerVersion, u32);
    check!(FeedProfileVersion, u32);
    check!(InstrumentSlot, u32);
    check!(StreamId, u32);
    check!(BookId, u32);
    check!(ConnectionId, u32);
    check!(ClockId, u32);
    check!(ConnectionEpoch, u64);
    check!(SubscriptionEpoch, u64);
    check!(BookEpoch, u64);
    check!(RecordNo, u64);
    check!(CaptureAttemptNo, u64);
    assert_eq!(OutputIndex::new(0).get(), 0);
    assert_eq!(
        RawSubIndex::new(u32::MAX).checked_next(),
        Err(E::CounterExhausted("RawSubIndex"))
    );
    assert_eq!(ArchiveId::new([0; 16]), Err(E::ZeroIdentifier("ArchiveId")));
    assert_eq!(
        CaptureSessionId::new([0; 16]),
        Err(E::ZeroIdentifier("CaptureSessionId"))
    );
    assert_eq!(ArchiveId::new([1; 16]).unwrap().as_bytes(), &[1; 16]);
}

#[test]
fn e16_epoch_expected_and_rollback() {
    let current = ConnectionEpoch::new(2).unwrap();
    let one = ConnectionEpoch::new(1).unwrap();
    let three = ConnectionEpoch::new(3).unwrap();
    assert_eq!(current.advance(one, three), Err(E::EpochMismatch));
    assert_eq!(current.advance(current, one), Err(E::EpochRollback));
    assert_eq!(current.advance(current, current), Err(E::EpochRollback));
    assert_eq!(current.advance(current, three), Ok(three));
}

#[test]
fn n01_n17_qualified_conversions_preserve_reference_and_units() {
    let f = fields();
    let spec = NumericSpec::new(f.clone()).unwrap();
    let price = spec
        .parse_price(&f.reference, &f.price_units, "100.10")
        .unwrap();
    assert_eq!(price.reference(), &f.reference);
    assert_eq!(price.ticks().get(), 2002);
    assert_eq!(spec.price_decimal(&price).unwrap().to_string(), "100.1");
    let quantity = spec
        .parse_quantity(&f.reference, &f.quantity_unit, "123")
        .unwrap();
    assert_eq!(quantity.reference(), &f.reference);
    let amount = spec
        .convert_quantity(&quantity, QuantityConversion::LinearBase(&f.base_asset))
        .unwrap();
    assert_eq!(amount.to_string(), "1.23");
    assert_eq!(
        spec.base_to_quantity(&f.reference, &f.base_asset, amount),
        Ok(quantity)
    );
}

#[test]
fn n18_units_unknown_nonlinear_and_metadata_guards() {
    let mut f = fields();
    f.quantity_to_base_multiplier = None;
    let spec = NumericSpec::new(f.clone()).unwrap();
    let quantity = spec
        .parse_quantity(&f.reference, &f.quantity_unit, "123")
        .unwrap();
    assert_eq!(
        spec.convert_quantity(&quantity, QuantityConversion::LinearBase(&f.base_asset)),
        Err(V::Numeric(N::UnknownMultiplier))
    );
    assert_eq!(
        spec.convert_quantity(&quantity, QuantityConversion::NonlinearValuation),
        Err(V::Numeric(N::UnsupportedConversion))
    );
    let wrong_unit = Token::new("OTHER").unwrap();
    assert_eq!(
        spec.convert_quantity(&quantity, QuantityConversion::LinearBase(&wrong_unit)),
        Err(V::UnitMismatch)
    );
    assert_eq!(
        spec.parse_quantity(&f.reference, &wrong_unit, "not-a-number"),
        Err(V::UnitMismatch)
    );
    f.quantity_unit = f.base_asset.clone();
    assert_eq!(
        NumericSpec::new(f.clone()),
        Err(V::Numeric(N::InvalidMultiplier))
    );
    f.quantity_to_base_multiplier = Some(ExactDecimal::ONE);
    assert_eq!(NumericSpec::new(f.clone()).unwrap().fields(), &f);
    f.price_increment = ExactDecimal::ZERO;
    assert_eq!(NumericSpec::new(f), Err(V::Numeric(N::InvalidIncrement)));
}

#[test]
fn n21_identity_and_spec_mismatch_precede_parsing_and_comparison() {
    let f = fields();
    let spec = NumericSpec::new(f.clone()).unwrap();
    let mut other = f.reference.clone();
    other.instrument.market = MarketKind::Perpetual;
    assert_eq!(
        spec.parse_price(&other, &f.price_units, "invalid"),
        Err(V::Identity(E::IdentityMismatch))
    );
    other = f.reference.clone();
    other.version = SpecVersion::new(2).unwrap();
    assert_eq!(
        spec.parse_price(&other, &f.price_units, "invalid"),
        Err(V::Identity(E::SpecMismatch))
    );
    let a = PricedValue::new(f.reference.clone(), PriceTicks::new(2).unwrap());
    let b = PricedValue::new(other.clone(), PriceTicks::new(2).unwrap());
    assert_eq!(a.checked_cmp(&b), Err(V::Identity(E::SpecMismatch)));
    let a = SizedValue::new(f.reference, QuantitySteps::new(1));
    let b = SizedValue::new(other, QuantitySteps::new(1));
    assert_eq!(a.checked_cmp(&b), Err(V::Identity(E::SpecMismatch)));
}
