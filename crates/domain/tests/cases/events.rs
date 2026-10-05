//! E04-E17 and structural A1/R4 vectors. No live decoder or clock.

use crate::support::fixtures::*;
use domain::artifact::ArtifactError;
use domain::event::*;
use domain::identity::*;
use domain::numeric::{NumericError, PriceTicks, QuantitySteps};

fn check(
    event: &EventEnvelope,
    source: &NormalizedFrame,
    prefix: &MemoryPrefix,
    apply: u64,
    earlier: &[EventCursor],
) -> Result<(), EventError> {
    event.validate(
        &EnvelopeContext {
            apply: record_ref(apply),
            binding: &source.source.binding,
            active: source.source.context,
            clock: clock(),
            artifact_refs: &[],
            prior_outputs_this_step: earlier,
        },
        prefix,
    )
}

#[test]
fn e04_e05_e08_source_application_ids_and_replay_are_distinct() {
    let raw = frame(10, vec![update(), update()], binding(1, Channel::BookNormal));
    let mut prefix = MemoryPrefix::through(13);
    prefix.insert_frame(raw.clone());
    let first = envelope(&raw, 0, 13, 0);
    let second = envelope(&raw, 1, 13, 1);
    assert_eq!(first.source_candidate.raw, raw_id(10));
    assert_eq!(second.source_candidate.index.get(), 1);
    assert_eq!(first.event_id.cursor, cursor(13, 0));
    assert_eq!(second.event_id.cursor, cursor(13, 1));
    assert_eq!(first.available_at, cursor(13, 0));
    assert_eq!(first.as_of.record_frontier, record(13));
    assert_ne!(first.event_id, second.event_id);
    assert_eq!(check(&first, &raw, &prefix, 13, &[]), Ok(()));
    assert_eq!(check(&second, &raw, &prefix, 13, &[]), Ok(()));
    assert_eq!(envelope(&raw, 0, 13, 0), first);
    assert_eq!(validate_dense_outputs(record_ref(13), &[first, second]), Ok(()));
}

#[test]
fn e06_redelivery_checks_all_canonical_fields() {
    let raw = frame(10, vec![snapshot()], binding(1, Channel::BookNormal));
    let event = envelope(&raw, 0, 11, 0);
    assert_eq!(classify_delivery(None, &event), Ok(Delivery::New));
    assert_eq!(
        classify_delivery(Some(&event), &event),
        Ok(Delivery::IdenticalDuplicate)
    );
    let mut changed = event.clone();
    changed.received.unix_ns = LocalUnixNs::new(999);
    assert_eq!(
        classify_delivery(Some(&event), &changed),
        Err(EventError::Identity(IdentityError::IdentityConflict))
    );
}

#[test]
fn e07_record_and_output_order_retain_prefix_on_error() {
    let mut order = RecordedOrder::default();
    assert_eq!(order.admit(record(3)), Err(EventError::RecordOrderError));
    assert_eq!(order.last(), None);
    assert_eq!(order.admit(record(1)), Ok(()));
    assert_eq!(order.admit(record(1)), Err(EventError::RecordOrderError));
    assert_eq!(order.admit(record(3)), Err(EventError::RecordOrderError));
    assert_eq!(order.last(), Some(record(1)));
    assert_eq!(order.admit(record(2)), Ok(()));
    let raw = frame(10, vec![update(), update()], binding(1, Channel::BookNormal));
    let events = [envelope(&raw, 0, 13, 0), envelope(&raw, 1, 13, 2)];
    assert_eq!(
        validate_dense_outputs(record_ref(13), &events),
        Err(EventError::SubEventOrderError)
    );
}

#[test]
fn e09_v_r4_timeline_same_normalizer_then_new_revision() {
    let mut timeline = ContextTimeline::default();
    let mut history = MemoryPrefix::default();
    let bootstrap = InputContext::decode(0, 0, 4, false).unwrap();
    assert_eq!(bootstrap, InputContext::Bootstrap);
    assert_eq!(
        timeline.activate(record(4), bootstrap, active(1, 1), artifact_ref(1), &history),
        Ok(())
    );
    history.configs.insert(active(1, 1).config);
    history.normalizers.insert(active(1, 1).normalizer, artifact_ref(1));
    assert_eq!(timeline.check(record(5), InputContext::Active(active(1, 1))), Ok(()));
    assert_eq!(
        timeline.activate(
            record(20), InputContext::Active(active(1, 1)), active(2, 1), artifact_ref(1), &history,
        ),
        Ok(())
    );
    history.configs.insert(active(2, 1).config);
    assert_eq!(timeline.active(), Some(active(2, 1)));
    assert_eq!(
        timeline.check(record(21), InputContext::Active(active(1, 1))),
        Err(EventError::ContextMismatch)
    );
    let mut first_raw = frame(21, vec![trade()], binding(3, Channel::Trades));
    first_raw.source.context = active(2, 1);
    let first = envelope(&first_raw, 0, 21, 0);
    assert_eq!(
        timeline.activate(
            record(30), InputContext::Active(active(2, 1)), active(3, 2), artifact_ref(2), &history,
        ),
        Ok(())
    );
    let mut second_raw = frame(31, vec![trade()], binding(3, Channel::Trades));
    second_raw.source.context = active(3, 2);
    let second = envelope(&second_raw, 0, 31, 0);
    assert_eq!(first.event_id.normalizer.get(), 1);
    assert_eq!(second.event_id.normalizer.get(), 2);
    assert_eq!(second.available_at, cursor(31, 0));
    assert!(first.event_id.cursor < second.event_id.cursor);
    let mut prefix = MemoryPrefix::through(31);
    prefix.insert_frame(first_raw.clone());
    prefix.insert_frame(second_raw.clone());
    assert_eq!(check(&first, &first_raw, &prefix, 21, &[]), Ok(()));
    assert_eq!(check(&second, &second_raw, &prefix, 31, &[]), Ok(()));
}

#[test]
fn v_r4_bootstrap_invalid_stale_context_and_rebinding() {
    for kind in 5..=7 {
        assert_eq!(InputContext::decode(0, 0, kind, false), Err(EventError::InvalidBootstrapContext));
    }
    for (config, normalizer) in [(0, 1), (1, 0)] {
        assert_eq!(InputContext::decode(config, normalizer, 4, false), Err(EventError::InvalidBootstrapContext));
    }
    assert_eq!(InputContext::decode(0, 0, 4, true), Err(EventError::InvalidBootstrapContext));
    let mut timeline = ContextTimeline::default();
    let mut history = MemoryPrefix::default();
    timeline.activate(record(4), InputContext::Bootstrap, active(1, 1), artifact_ref(1), &history).unwrap();
    history.configs.insert(active(1, 1).config);
    history.normalizers.insert(active(1, 1).normalizer, artifact_ref(1));
    let saved = timeline.clone();
    assert_eq!(
        timeline.activate(record(5), InputContext::Active(active(1, 1)), active(2, 1), artifact_ref(2), &history),
        Err(EventError::Artifact(ArtifactError::ArtifactIdentityConflict))
    );
    assert_eq!(timeline, saved);
    assert_eq!(
        timeline.activate(record(5), InputContext::Active(active(1, 1)), active(1, 1), artifact_ref(1), &history),
        Err(EventError::Identity(IdentityError::IdentityConflict))
    );
    assert_eq!(timeline, saved);
}

#[test]
fn e11_e12_clocks_units_and_unix_jumps_are_not_order() {
    let earlier = MonotonicSample { scope: clock(), ns: MonotonicNs::new(10) };
    let later = MonotonicSample { scope: clock(), ns: MonotonicNs::new(15) };
    assert_eq!(later.elapsed_since(earlier).unwrap().get(), 5);
    let mut foreign = earlier;
    foreign.scope.clock = ClockId::new(2).unwrap();
    assert_eq!(later.elapsed_since(foreign), Err(EventError::IncomparableClock));
    assert_eq!(earlier.elapsed_since(later), Err(EventError::TimeOrderError));
    assert_eq!(TimestampUnit::Seconds.to_nanoseconds(2), Ok(2_000_000_000));
    assert_eq!(TimestampUnit::Millis.to_nanoseconds(-2), Ok(-2_000_000));
    assert_eq!(TimestampUnit::Micros.to_nanoseconds(2), Ok(2000));
    assert_eq!(TimestampUnit::Nanos.to_nanoseconds(i64::MAX), Ok(i64::MAX));
    assert_eq!(TimestampUnit::Seconds.to_nanoseconds(i64::MAX), Err(EventError::TimeOverflow));
    let mut raw = frame(10, vec![trade()], binding(3, Channel::Trades));
    raw.source.received.unix_ns = LocalUnixNs::new(i64::MIN);
    raw.outputs[0].timestamp = SourceTimestamp::Known {
        value: 200, unit: TimestampUnit::Millis, origin: Token::new("synthetic").unwrap(),
    };
    let first = envelope(&raw, 0, 10, 0);
    raw.source.raw = raw_id(11);
    raw.outputs[0].timestamp = SourceTimestamp::Known {
        value: 100, unit: TimestampUnit::Millis, origin: Token::new("synthetic").unwrap(),
    };
    let second = envelope(&raw, 0, 11, 0);
    assert!(first.available_at < second.available_at);
}

#[test]
fn e13_causal_frontiers_references_and_availability() {
    let raw = frame(10, vec![snapshot()], binding(1, Channel::BookNormal));
    let mut prefix = MemoryPrefix::through(13);
    prefix.insert_frame(raw.clone());
    let valid = envelope(&raw, 0, 13, 0);
    let mut bad = valid.clone();
    bad.as_of.record_frontier = record(14);
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::FutureCausalReference));
    bad = valid.clone();
    bad.as_of.record_frontier = record(12);
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::CausalFrontierMismatch));
    bad = valid.clone();
    bad.available_at = cursor(10, 0);
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::AvailabilityMismatch));
    bad = valid.clone();
    bad.as_of.prior_effects = vec![cursor(14, 0)];
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::FutureCausalReference));
    bad.as_of.prior_effects = vec![cursor(9, 0)];
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::MissingCausalReference));
    prefix.effects.insert(EventRef { archive: archive(), cursor: cursor(9, 0) });
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Ok(()));
    bad.as_of.prior_effects = vec![cursor(9, 0), cursor(9, 0)];
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::SubEventOrderError));
}

#[test]
fn same_step_earlier_effect_is_available_but_later_is_not() {
    let raw = frame(10, vec![update(), update()], binding(1, Channel::BookNormal));
    let mut prefix = MemoryPrefix::through(13);
    prefix.insert_frame(raw.clone());
    let mut event = envelope(&raw, 1, 13, 1);
    event.as_of.prior_effects = vec![cursor(13, 0)];
    assert_eq!(check(&event, &raw, &prefix, 13, &[cursor(13, 0)]), Ok(()));
    event.as_of.prior_effects = vec![cursor(13, 1)];
    assert_eq!(check(&event, &raw, &prefix, 13, &[]), Err(EventError::FutureCausalReference));
}

#[test]
fn e14_unknown_is_preserved_and_payload_reinterpretation_rejected() {
    let raw = frame(10, vec![trade()], binding(3, Channel::Trades));
    let mut prefix = MemoryPrefix::through(10);
    prefix.insert_frame(raw.clone());
    let mut event = envelope(&raw, 0, 10, 0);
    assert_eq!(event.source_timestamp, SourceTimestamp::Unknown);
    assert_eq!(check(&event, &raw, &prefix, 10, &[]), Ok(()));
    let MarketPayload::Trade(value) = &mut event.payload else { panic!("fixture must be trade") };
    assert_eq!(value.aggressor, Aggressor::Unknown);
    assert_eq!(value.rpi, RpiAttribute::Unknown);
    assert_eq!(value.book_link, TradeBookLink::Unknown);
    value.aggressor = Aggressor::Buy;
    assert_eq!(check(&event, &raw, &prefix, 10, &[]), Err(EventError::SourceMismatch));
}

#[test]
fn e17_n20_level_structure_and_zero_deletion_distinction() {
    assert_eq!(snapshot().validate(), Ok(()));
    let empty = MarketPayload::Snapshot(vec![]);
    assert_eq!(empty.validate(), Ok(()));
    assert!(!empty.two_sided_snapshot());
    let duplicate = MarketPayload::Snapshot(vec![level(Side::Bid, 2, 1), level(Side::Bid, 2, 1)]);
    assert_eq!(duplicate.validate(), Err(EventError::DuplicateLevel));
    let unordered = MarketPayload::Snapshot(vec![level(Side::Ask, 3, 1), level(Side::Bid, 2, 1)]);
    assert_eq!(unordered.validate(), Err(EventError::SnapshotOrderError));
    let zero = MarketPayload::Snapshot(vec![level(Side::Bid, 2, 0)]);
    assert_eq!(zero.validate(), Err(EventError::Numeric(NumericError::ZeroLevelQuantity)));
    let deletion = LevelChange::Delete { side: Side::Bid, price: PriceTicks::new(2).unwrap() };
    assert_eq!(MarketPayload::Update(vec![deletion.clone()]).validate(), Ok(()));
    assert_eq!(MarketPayload::Update(vec![deletion.clone(), deletion]).validate(), Err(EventError::DuplicateLevel));
    let oversized = MarketPayload::Snapshot(vec![level(Side::Bid, 2, 1); 4097]);
    assert_eq!(oversized.validate(), Err(EventError::EventTooLarge));
    let mut zero_trade = trade();
    let MarketPayload::Trade(value) = &mut zero_trade else { panic!("trade fixture") };
    value.quantity = QuantitySteps::new(0);
    assert_eq!(zero_trade.validate(), Err(EventError::Numeric(NumericError::ZeroTradeQuantity)));
}

#[test]
fn v_r1_mixed_and_invalid_whole_frame_have_no_partial_acceptance() {
    let mixed = frame(10, vec![snapshot(), update()], binding(1, Channel::BookNormal));
    assert_eq!(mixed.validate(), Err(EventError::MixedFrameUnsupported));
    let bad_update = MarketPayload::Update(vec![
        LevelChange::Set(level(Side::Bid, 2, 1)),
        LevelChange::Set(level(Side::Bid, 2, 2)),
    ]);
    let atomic = frame(10, vec![update(), bad_update], binding(1, Channel::BookNormal));
    assert_eq!(atomic.validate(), Err(EventError::DuplicateLevel));
    let empty = frame(10, vec![], binding(1, Channel::BookNormal));
    assert_eq!(empty.validate(), Ok(FrameKind::NoMarketData));
}

#[test]
fn e03_missing_raw_source_index_and_epoch_are_rejected() {
    let raw = frame(10, vec![snapshot()], binding(1, Channel::BookNormal));
    let mut prefix = MemoryPrefix::through(13);
    let valid = envelope(&raw, 0, 13, 0);
    assert_eq!(check(&valid, &raw, &prefix, 13, &[]), Err(EventError::MissingRawInput));
    prefix.insert_frame(raw.clone());
    let mut bad = valid.clone();
    bad.source_candidate.index = RawSubIndex::new(1);
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::SubEventOrderError));
    bad = valid.clone();
    bad.binding.tag.connection = ConnectionEpoch::new(2).unwrap();
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::Identity(IdentityError::EpochMismatch)));
    bad = valid;
    bad.received.monotonic.ns = MonotonicNs::new(11);
    assert_eq!(check(&bad, &raw, &prefix, 13, &[]), Err(EventError::SourceMismatch));
}
