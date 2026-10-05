//! V-R5-* accounting vectors. No runtime queue or guessed exchange sequence.

use crate::support::accounting::*;
use crate::support::fixtures;
use crate::support::health::ModelError;
use crate::support::scenario::Scenario;
use domain::identity::*;
use domain::record::*;

fn target(range: Option<(u64, u64)>, count: Option<u64>) -> GapTarget {
    GapTarget {
        stream: StreamId::new(1).unwrap(),
        tag: fixtures::binding(1, Channel::BookNormal).tag,
        range: range.map(|(a, b)| {
            (
                CaptureAttemptNo::new(a).unwrap(),
                CaptureAttemptNo::new(b).unwrap(),
            )
        }),
        loss_count: count,
    }
}

fn raw(state: &mut LossState, n: u64) -> Result<RawAccounting, LossError> {
    state.raw(CaptureAttemptNo::new(n).unwrap(), target(None, None).tag)
}

fn gap(
    state: &mut LossState,
    range: Option<(u64, u64)>,
    count: Option<u64>,
) -> Result<(), LossError> {
    let target = target(range, count);
    state.gap(
        &target,
        Reason::QueueOverflow,
        RecordNo::new(11).unwrap(),
        target.tag,
    )
}

#[test]
fn v_r5_one_use_and_no_jump_consumption() {
    for next in [2, 3] {
        let mut state = LossState::default();
        raw(&mut state, 1).unwrap();
        gap(&mut state, None, None).unwrap();
        assert_eq!(state.accounted_frontier, 1);
        let accounting = raw(&mut state, next).unwrap();
        assert_eq!(
            accounting.inferred_interval,
            if next == 3 { Some((2, 2)) } else { None }
        );
        assert_eq!(accounting.recorded_count, None);
        assert_eq!(state.window, None);
        assert_eq!(state.accounted_frontier, next);
        let before = state.clone();
        assert_eq!(raw(&mut state, 100), Err(LossError::UnaccountedAttemptGap));
        assert_eq!(state, before);
    }
}

#[test]
fn v_r5_initial_missing_and_initial_unknown_gap() {
    let mut state = LossState::default();
    assert_eq!(raw(&mut state, 3), Err(LossError::UnaccountedAttemptGap));
    assert_eq!(state, LossState::default());
    gap(&mut state, None, None).unwrap();
    let result = raw(&mut state, 3).unwrap();
    assert_eq!(result.inferred_interval, Some((1, 2)));
    assert_eq!(result.recorded_count, None);
    assert_eq!(state.accounted_frontier, 3);
    assert_eq!(state.window, None);
}

#[test]
fn v_r5_known_ranges_cover_exact_missing_attempts() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    gap(&mut state, Some((2, 3)), Some(2)).unwrap();
    assert_eq!(state.accounted_frontier, 3);
    raw(&mut state, 4).unwrap();
    assert_eq!(state.accounted_frontier, 4);
    assert_eq!(state.finish(), Ok(()));
}

#[test]
fn v_r5_overlap_raw_overlap_loss_and_coverage_gap_retain_state() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    let before = state.clone();
    assert_eq!(
        gap(&mut state, Some((1, 2)), Some(2)),
        Err(LossError::LossOverlap)
    );
    assert_eq!(state, before);
    assert_eq!(
        gap(&mut state, Some((3, 4)), Some(2)),
        Err(LossError::LossCoverageGap)
    );
    assert_eq!(state, before);
    gap(&mut state, Some((2, 3)), Some(2)).unwrap();
    let before = state.clone();
    assert_eq!(
        gap(&mut state, Some((3, 4)), Some(2)),
        Err(LossError::LossOverlap)
    );
    assert_eq!(state, before);
}

#[test]
fn v_r5_known_unknown_count_and_zero_are_not_guessed() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    assert_eq!(
        gap(&mut state, Some((2, 3)), Some(1)),
        Err(LossError::Shape(RecordError::LossCountMismatch))
    );
    assert_eq!(state.accounted_frontier, 1);
    assert_eq!(
        gap(&mut state, None, Some(0)),
        Err(LossError::Shape(RecordError::InvalidLossCount))
    );
    assert_eq!(state.window, None);
    gap(&mut state, None, Some(2)).unwrap();
    let before = state.clone();
    assert_eq!(raw(&mut state, 3), Err(LossError::LossCountMismatch));
    assert_eq!(state, before);
    let result = raw(&mut state, 4).unwrap();
    assert_eq!(result.inferred_interval, Some((2, 3)));
    assert_eq!(result.recorded_count, Some(2));
}

#[test]
fn v_r5_source_gap_never_authorizes_local_attempt_holes() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    let source = target(None, None);
    state
        .gap(
            &source,
            Reason::SourceGap,
            RecordNo::new(11).unwrap(),
            source.tag,
        )
        .unwrap();
    assert_eq!(state.window, None);
    assert_eq!(raw(&mut state, 3), Err(LossError::UnaccountedAttemptGap));
    for bad in [target(Some((2, 3)), Some(2)), target(None, Some(2))] {
        assert_eq!(
            state.gap(&bad, Reason::SourceGap, RecordNo::new(11).unwrap(), bad.tag),
            Err(LossError::Shape(RecordError::InvalidLossScope))
        );
    }
    gap(&mut state, None, None).unwrap();
    let before = state.clone();
    state
        .gap(
            &source,
            Reason::SourceGap,
            RecordNo::new(12).unwrap(),
            source.tag,
        )
        .unwrap();
    assert_eq!(state, before);
}

#[test]
fn v_r5_second_unknown_known_and_scope_change_are_ambiguous() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    gap(&mut state, None, None).unwrap();
    let before = state.clone();
    assert_eq!(
        gap(&mut state, None, None),
        Err(LossError::AmbiguousLossWindow)
    );
    assert_eq!(
        gap(&mut state, Some((2, 3)), Some(2)),
        Err(LossError::AmbiguousLossWindow)
    );
    assert_eq!(state.scope_change(), Err(LossError::GapScopeTransition));
    assert_eq!(state.finish(), Err(LossError::UnresolvedLossWindow));
    let mut changed = target(None, None).tag;
    changed.connection = ConnectionEpoch::new(2).unwrap();
    assert_eq!(
        state.raw(CaptureAttemptNo::new(2).unwrap(), changed),
        Err(LossError::GapScopeTransition)
    );
    assert_eq!(state, before);
}

#[test]
fn v_r5_epoch_does_not_reset_attempts_and_exhaustion_does_not_wrap() {
    let mut state = LossState::default();
    raw(&mut state, 1).unwrap();
    state.scope_change().unwrap();
    let mut changed = target(None, None).tag;
    changed.connection = ConnectionEpoch::new(2).unwrap();
    state
        .raw(CaptureAttemptNo::new(2).unwrap(), changed)
        .unwrap();
    assert_eq!(state.accounted_frontier, 2);
    assert_eq!(
        state.raw(CaptureAttemptNo::new(1).unwrap(), changed),
        Err(LossError::AttemptOrderError)
    );
    let mut boundary = LossState {
        accounted_frontier: u64::MAX,
        window: None,
    };
    assert_eq!(
        raw(&mut boundary, 1),
        Err(LossError::AttemptCounterExhausted)
    );
    assert_eq!(boundary.accounted_frontier, u64::MAX);
}

#[test]
fn v_r5_model_retains_semantic_prefix_after_unaccounted_raw() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![], 10).unwrap();
    s.gap(Reason::QueueOverflow, None, None, 11).unwrap();
    let raw = s.raw_input(vec![], 12, Some(3), None);
    s.submit(Record::RawInput(raw)).unwrap();
    let before = s.model.clone();
    let raw = s.raw_input(vec![], 13, Some(100), None);
    assert_eq!(
        s.submit(Record::RawInput(raw)),
        Err(ModelError::Loss(LossError::UnaccountedAttemptGap))
    );
    assert_eq!(s.model.last_record.get(), 12);
    assert_eq!(s.model.evaluation_ns, 12);
    assert_eq!(s.model.streams, before.streams);
    assert_eq!(s.stream().loss.accounted_frontier, 3);
    assert_eq!(s.stream().loss.window, None);
}

#[test]
fn v_r5_model_open_window_blocks_config_activation_atomically() {
    let mut s = Scenario::initial(fixtures::policy());
    s.raw(vec![], 10).unwrap();
    s.gap(Reason::QueueOverflow, None, None, 11).unwrap();
    let before = s.model.clone();
    assert_eq!(
        s.config_change(2, 1, fixtures::policy(), 12),
        Err(ModelError::Loss(LossError::GapScopeTransition))
    );
    assert_eq!(s.model.context().unwrap().config.get(), 1);
    assert_eq!(s.model.last_record.get(), 11);
    assert_eq!(s.model.streams, before.streams);
}
