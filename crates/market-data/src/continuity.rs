use crate::decoder::{Action, Books50Frame};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuityRule {
    SnapshotInterval,
    PreviousSeqEqualsPseq,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuityOutcome {
    AnchorCandidate {
        seq: u64,
    },
    Continuous {
        rule: ContinuityRule,
        previous_seq: u64,
        current_pseq: u64,
        current_seq: u64,
    },
    DuplicateDiagnostic {
        seq: u64,
        pseq: u64,
    },
    Gap {
        previous_seq: u64,
        current_pseq: u64,
        current_seq: u64,
    },
    ResetOrDiscontinuity {
        previous_seq: u64,
        current_pseq: u64,
        current_seq: u64,
        pseq_zero_hint: bool,
    },
    SnapshotIntervalMismatch {
        snapshot_seq: u64,
        current_pseq: u64,
        current_seq: u64,
    },
    NeedsSnapshot {
        current_seq: u64,
    },
    UnexpectedSnapshot {
        previous_seq: u64,
        current_seq: u64,
    },
}

/// Deterministic visible-push continuity classification.
///
/// One instance is intended for one already-qualified regular books50 stream.
/// It never performs I/O, state repair, or book mutation.
#[derive(Clone, Debug, Default)]
pub struct ContinuityClassifier {
    previous: Option<Books50Frame>,
    invalidated: bool,
}

impl ContinuityClassifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_invalidated(&self) -> bool {
        self.invalidated
    }

    pub fn clear_for_new_generation(&mut self) {
        self.previous = None;
        self.invalidated = false;
    }

    pub fn observe(&mut self, current: &Books50Frame) -> ContinuityOutcome {
        if current.action == Action::Snapshot {
            if self.previous.is_none() {
                self.previous = Some(current.clone());
                self.invalidated = false;
                return ContinuityOutcome::AnchorCandidate { seq: current.seq };
            }

            let previous_seq = self.previous.as_ref().map_or(0, |frame| frame.seq);
            self.previous = None;
            self.invalidated = true;
            return ContinuityOutcome::UnexpectedSnapshot {
                previous_seq,
                current_seq: current.seq,
            };
        }

        if self.invalidated {
            return ContinuityOutcome::NeedsSnapshot {
                current_seq: current.seq,
            };
        }

        let Some(previous) = self.previous.as_ref() else {
            self.invalidated = true;
            return ContinuityOutcome::NeedsSnapshot {
                current_seq: current.seq,
            };
        };

        if previous == current {
            return ContinuityOutcome::DuplicateDiagnostic {
                seq: current.seq,
                pseq: current.pseq,
            };
        }

        let previous_action = previous.action;
        let previous_seq = previous.seq;

        match previous_action {
            Action::Snapshot => {
                if current.pseq <= previous_seq && previous_seq <= current.seq {
                    self.previous = Some(current.clone());
                    ContinuityOutcome::Continuous {
                        rule: ContinuityRule::SnapshotInterval,
                        previous_seq,
                        current_pseq: current.pseq,
                        current_seq: current.seq,
                    }
                } else {
                    self.previous = None;
                    self.invalidated = true;
                    ContinuityOutcome::SnapshotIntervalMismatch {
                        snapshot_seq: previous_seq,
                        current_pseq: current.pseq,
                        current_seq: current.seq,
                    }
                }
            }
            Action::Update => {
                if previous_seq == current.pseq {
                    self.previous = Some(current.clone());
                    ContinuityOutcome::Continuous {
                        rule: ContinuityRule::PreviousSeqEqualsPseq,
                        previous_seq,
                        current_pseq: current.pseq,
                        current_seq: current.seq,
                    }
                } else {
                    self.previous = None;
                    self.invalidated = true;
                    if current.seq < previous_seq {
                        ContinuityOutcome::ResetOrDiscontinuity {
                            previous_seq,
                            current_pseq: current.pseq,
                            current_seq: current.seq,
                            pseq_zero_hint: current.pseq == 0,
                        }
                    } else {
                        ContinuityOutcome::Gap {
                            previous_seq,
                            current_pseq: current.pseq,
                            current_seq: current.seq,
                        }
                    }
                }
            }
        }
    }
}
