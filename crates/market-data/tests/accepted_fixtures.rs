use market_data::{
    Action, BitgetMessage, Books50Frame, Category, ContinuityClassifier, ContinuityOutcome,
    ContinuityRule, DecodeError, FillSide, PublicTradeFrame, RpiFlag, Topic, decode_message,
};

const SNAPSHOT: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-snapshot.json");
const UPDATE: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-update.json");
const GAP: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-gap.json");
const DUPLICATE: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-duplicate.json");
const RESET: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-reset.json");
const EMPTY_LEVELS: &[u8] =
    include_bytes!("../../../tests/fixtures/bitget/books50-empty-levels.json");
const PUBLIC_TRADES: &[u8] = include_bytes!("../../../tests/fixtures/bitget/public-trades.json");
const ZERO_QUANTITY: &[u8] =
    include_bytes!("../../../tests/fixtures/bitget/books50-zero-quantity-unknown.json");
const RPI_SNAPSHOT: &[u8] =
    include_bytes!("../../../tests/fixtures/bitget/rpi-books50-snapshot.json");

fn books(bytes: &[u8]) -> Books50Frame {
    match decode_message(bytes).expect("accepted books50 fixture must decode") {
        BitgetMessage::Books50(frame) => frame,
        BitgetMessage::PublicTrade(_) => panic!("expected books50 frame"),
    }
}

fn trades(bytes: &[u8]) -> PublicTradeFrame {
    match decode_message(bytes).expect("accepted publicTrade fixture must decode") {
        BitgetMessage::PublicTrade(frame) => frame,
        BitgetMessage::Books50(_) => panic!("expected publicTrade frame"),
    }
}

#[test]
fn snapshot_preserves_wire_fields_and_lexical_values() {
    let frame = books(SNAPSHOT);

    assert_eq!(frame.category, Category::UsdtFutures);
    assert_eq!(frame.symbol, "BTCUSDT");
    assert_eq!(frame.topic, Topic::Books50);
    assert_eq!(frame.action, Action::Snapshot);
    assert_eq!(frame.seq, 1000);
    assert_eq!(frame.pseq, 0);
    assert_eq!(frame.source_timestamp.value, 1_770_000_000_000);
    assert_eq!(frame.source_timestamp.lexical, "1770000000000");
    assert_eq!(frame.envelope_timestamp.value, 1_770_000_000_001);
    assert_eq!(frame.asks[0].price.as_str(), "100.2");
    assert_eq!(frame.asks[0].quantity.as_str(), "1.2500");
    assert_eq!(frame.bids[1].price.as_str(), "100.0");
    assert_eq!(frame.bids[1].quantity.as_str(), "3.4000");
}

#[test]
fn all_regular_books50_fixtures_decode_without_canonical_effects() {
    for bytes in [UPDATE, GAP, DUPLICATE, RESET, EMPTY_LEVELS, ZERO_QUANTITY] {
        let frame = books(bytes);
        assert_eq!(frame.category, Category::UsdtFutures);
        assert_eq!(frame.symbol, "BTCUSDT");
        assert_eq!(frame.topic, Topic::Books50);
    }

    let update = books(UPDATE);
    let duplicate = books(DUPLICATE);
    assert_eq!(update, duplicate);

    let empty = books(EMPTY_LEVELS);
    assert!(empty.asks.is_empty());
    assert!(empty.bids.is_empty());

    let zero = books(ZERO_QUANTITY);
    assert_eq!(zero.asks.len(), 1);
    assert_eq!(zero.asks[0].price.as_str(), "100.2");
    assert_eq!(zero.asks[0].quantity.as_str(), "0");
}

#[test]
fn public_trade_preserves_documented_wire_fields() {
    let frame = trades(PUBLIC_TRADES);

    assert_eq!(frame.category, Category::UsdtFutures);
    assert_eq!(frame.symbol, "BTCUSDT");
    assert_eq!(frame.topic, Topic::PublicTrade);
    assert_eq!(frame.action, Action::Update);
    assert_eq!(frame.envelope_timestamp.value, 1_770_000_000_102);
    assert_eq!(frame.trades.len(), 2);

    let first = &frame.trades[0];
    assert_eq!(first.price.as_str(), "100.1");
    assert_eq!(first.size.as_str(), "0.0100");
    assert_eq!(first.execution_id, "9001");
    assert_eq!(first.correlation_id, "7001");
    assert_eq!(first.fill_side, FillSide::Buy);
    assert_eq!(first.timestamp.value, 1_770_000_000_100);
    assert_eq!(first.timestamp.lexical, "1770000000100");
    assert_eq!(first.is_rpi, RpiFlag::No);

    let second = &frame.trades[1];
    assert_eq!(second.fill_side, FillSide::Sell);
    assert_eq!(second.is_rpi, RpiFlag::Yes);
}

#[test]
fn rpi_two_quantity_profile_is_rejected_at_the_boundary() {
    let error = decode_message(RPI_SNAPSHOT).expect_err("RPI profile must stay unsupported");
    assert_eq!(
        error,
        DecodeError::UnsupportedProfile {
            topic: "rpi-books50".to_owned()
        }
    );
}

#[test]
fn snapshot_then_first_update_uses_the_documented_interval_rule() {
    let snapshot = books(SNAPSHOT);
    let mut first_update = books(UPDATE);
    first_update.pseq = 999;
    first_update.seq = 1005;

    let mut classifier = ContinuityClassifier::new();
    assert_eq!(
        classifier.observe(&snapshot),
        ContinuityOutcome::AnchorCandidate { seq: 1000 }
    );
    assert_eq!(
        classifier.observe(&first_update),
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::SnapshotInterval,
            previous_seq: 1000,
            current_pseq: 999,
            current_seq: 1005,
        }
    );
}

#[test]
fn update_to_update_checks_previous_seq_against_current_pseq() {
    let snapshot = books(SNAPSHOT);
    let update = books(UPDATE);
    let empty = books(EMPTY_LEVELS);

    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&empty),
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::PreviousSeqEqualsPseq,
            previous_seq: 1001,
            current_pseq: 1001,
            current_seq: 1002,
        }
    );
}

#[test]
fn gap_invalidates_and_does_not_auto_recover_on_a_following_update() {
    let snapshot = books(SNAPSHOT);
    let update = books(UPDATE);
    let gap = books(GAP);
    let empty = books(EMPTY_LEVELS);

    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&gap),
        ContinuityOutcome::Gap {
            previous_seq: 1001,
            current_pseq: 1003,
            current_seq: 1004,
        }
    );
    assert!(classifier.is_invalidated());
    assert_eq!(
        classifier.observe(&empty),
        ContinuityOutcome::NeedsSnapshot { current_seq: 1002 }
    );
    assert!(classifier.is_invalidated());
}

#[test]
fn reset_fixture_is_a_conservative_discontinuity_not_a_pseq_zero_contract() {
    let snapshot = books(SNAPSHOT);
    let update = books(UPDATE);
    let reset = books(RESET);

    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&reset),
        ContinuityOutcome::ResetOrDiscontinuity {
            previous_seq: 1001,
            current_pseq: 0,
            current_seq: 12,
            pseq_zero_hint: true,
        }
    );

    let mut nonzero_hint = reset.clone();
    nonzero_hint.pseq = 77;
    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&nonzero_hint),
        ContinuityOutcome::ResetOrDiscontinuity {
            previous_seq: 1001,
            current_pseq: 77,
            current_seq: 12,
            pseq_zero_hint: false,
        }
    );
}

#[test]
fn duplicate_fixture_is_only_a_project_diagnostic_case() {
    let snapshot = books(SNAPSHOT);
    let update = books(UPDATE);
    let duplicate = books(DUPLICATE);
    let empty = books(EMPTY_LEVELS);

    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&duplicate),
        ContinuityOutcome::DuplicateDiagnostic {
            seq: 1001,
            pseq: 1000,
        }
    );
    assert_eq!(
        classifier.observe(&empty),
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::PreviousSeqEqualsPseq,
            previous_seq: 1001,
            current_pseq: 1001,
            current_seq: 1002,
        }
    );
}

#[test]
fn classifier_does_not_require_observing_every_intermediate_book_event() {
    let snapshot = books(SNAPSHOT);
    let update = books(UPDATE);
    let mut coalesced_visible_push = books(EMPTY_LEVELS);
    coalesced_visible_push.pseq = 1001;
    coalesced_visible_push.seq = 1010;

    let mut classifier = ContinuityClassifier::new();
    let _ = classifier.observe(&snapshot);
    let _ = classifier.observe(&update);
    assert_eq!(
        classifier.observe(&coalesced_visible_push),
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::PreviousSeqEqualsPseq,
            previous_seq: 1001,
            current_pseq: 1001,
            current_seq: 1010,
        }
    );
}
