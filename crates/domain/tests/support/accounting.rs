//! Memory-only R5 reference accounting. No queue or recording runtime.

use domain::identity::{CaptureAttemptNo, EpochTag, RecordNo};
use domain::record::{GapTarget, Reason, RecordError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LossError {
    Shape(RecordError),
    LossOverlap,
    LossCoverageGap,
    LossCountMismatch,
    AmbiguousLossWindow,
    UnaccountedAttemptGap,
    AttemptOrderError,
    GapScopeTransition,
    UnresolvedLossWindow,
    AttemptCounterExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LossWindow {
    pub gap_record: RecordNo,
    pub left: u64,
    pub tag: EpochTag,
    pub recorded_count: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LossState {
    pub accounted_frontier: u64,
    pub window: Option<LossWindow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawAccounting {
    pub inferred_interval: Option<(u64, u64)>,
    pub recorded_count: Option<u64>,
}

impl LossState {
    pub fn gap(
        &mut self,
        target: &GapTarget,
        reason: Reason,
        record: RecordNo,
        current: EpochTag,
    ) -> Result<(), LossError> {
        target.validate(reason).map_err(LossError::Shape)?;
        if reason != Reason::QueueOverflow {
            return Ok(());
        }
        if target.tag != current {
            return Err(LossError::GapScopeTransition);
        }
        if self.window.is_some() {
            return Err(LossError::AmbiguousLossWindow);
        }
        let expected = self.accounted_frontier.checked_add(1).ok_or(LossError::AttemptCounterExhausted)?;
        if let Some((first, last)) = target.range {
            if first.get() <= self.accounted_frontier {
                return Err(LossError::LossOverlap);
            }
            if first.get() != expected {
                return Err(LossError::LossCoverageGap);
            }
            self.accounted_frontier = last.get();
        } else {
            self.window = Some(LossWindow {
                gap_record: record,
                left: self.accounted_frontier,
                tag: target.tag,
                recorded_count: target.loss_count,
            });
        }
        Ok(())
    }

    pub fn raw(&mut self, attempt: CaptureAttemptNo, tag: EpochTag) -> Result<RawAccounting, LossError> {
        let expected = self.accounted_frontier.checked_add(1).ok_or(LossError::AttemptCounterExhausted)?;
        let value = attempt.get();
        if value <= self.accounted_frontier {
            return Err(LossError::AttemptOrderError);
        }
        let result = if let Some(window) = &self.window {
            if window.tag != tag {
                return Err(LossError::GapScopeTransition);
            }
            let count = value - expected;
            if window.recorded_count.is_some_and(|n| n != count) {
                return Err(LossError::LossCountMismatch);
            }
            RawAccounting {
                inferred_interval: (count > 0).then_some((expected, value - 1)),
                recorded_count: window.recorded_count,
            }
        } else {
            if value != expected {
                return Err(LossError::UnaccountedAttemptGap);
            }
            RawAccounting { inferred_interval: None, recorded_count: None }
        };
        self.accounted_frontier = value;
        self.window = None;
        Ok(result)
    }

    pub fn scope_change(&self) -> Result<(), LossError> {
        if self.window.is_some() {
            return Err(LossError::GapScopeTransition);
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<(), LossError> {
        if self.window.is_some() {
            return Err(LossError::UnresolvedLossWindow);
        }
        Ok(())
    }
}
