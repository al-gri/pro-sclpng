//! Memory-only WAL recovery composed with the existing transactional reference
//! model. CRC framing is not canonical applicability or storage completion.

use domain::identity::{RecordNo, SegmentNo, StreamId};
use domain::record::*;

use super::binary::{Crc32, Error, ErrorKind};
use super::health::{HealthModel, ModelError, StepResult};
use super::model_env::ModelEnv;
use super::wal::{decode_frame, scan_frame};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Completion {
    NoArchive,
    ValidPrefixIncomplete,
    TruncatedTail,
    SegmentSealedArchiveIncomplete,
    Invalid,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryFault {
    Binary(Error),
    OrderOrChainError,
    TrailingDataError,
    Semantic(ModelError),
    OffsetOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    pub segment: usize,
    pub local_frame_start: usize,
    pub absolute_frame_start: u64,
    pub fault: RecoveryFault,
}

#[derive(Clone, Debug)]
pub struct Recovery {
    pub completion: Completion,
    pub input_quality: Option<InputQuality>,
    pub last_good_offset: u64,
    pub framing_good_offset: u64,
    pub last_record: Option<RecordNo>,
    pub failure: Option<Failure>,
    pub unresolved_loss_streams: Vec<StreamId>,
    pub frames: Vec<RecordFrame>,
    pub steps: Vec<StepResult>,
    pub model: Option<HealthModel>,
    pub env: ModelEnv,
}

impl Recovery {
    fn new(env: ModelEnv) -> Self {
        Self {
            completion: Completion::NoArchive,
            input_quality: None,
            last_good_offset: 0,
            framing_good_offset: 0,
            last_record: None,
            failure: None,
            unresolved_loss_streams: Vec::new(),
            frames: Vec::new(),
            steps: Vec::new(),
            model: None,
            env,
        }
    }

    fn fail(&mut self, segment: usize, local: usize, absolute: u64, fault: RecoveryFault) {
        self.completion = if matches!(fault, RecoveryFault::Binary(Error {
            kind: ErrorKind::TruncatedTail, ..
        })) {
            Completion::TruncatedTail
        } else {
            Completion::Invalid
        };
        // Preserve the last accepted data/prefix while explicitly disabling
        // canonical use. Detailed byte diagnostics live in failure, not a GAP.
        if let Some(model) = &mut self.model {
            if model.blocked.is_none() {
                model.blocked = Some(ModelError::Record(RecordError::InvalidPayload("WAL.input")));
            }
        }
        self.failure = Some(Failure {
            segment,
            local_frame_start: local,
            absolute_frame_start: absolute,
            fault,
        });
    }

    fn loss_diagnostics(&mut self) {
        self.unresolved_loss_streams = self.model.as_ref().map_or_else(Vec::new, |model| {
            model.streams.iter().filter_map(|(id, state)| {
                state.loss.window.as_ref().map(|_| *id)
            }).collect()
        });
    }
}

#[derive(Clone, Debug)]
struct SealLink {
    segment: SegmentNo,
    record: RecordNo,
    checksum: u32,
    final_segment: bool,
}

/// Segments are already ordered, immutable slices. This function neither opens
/// files nor guesses a missing segment. All offsets are checked before use.
pub fn recover(segments: &[&[u8]], env: ModelEnv) -> Recovery {
    let mut out = Recovery::new(env);
    if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
        return out;
    }
    let mut global_crc = Crc32::default();
    let mut global_count = 0_u64;
    let mut absolute = 0_u64;
    let mut seal: Option<SealLink> = None;
    let mut any_gap = false;
    let mut finished = false;

    for (segment_index, bytes) in segments.iter().enumerate() {
        let Ok(segment_number) = u32::try_from(segment_index) else {
            out.fail(segment_index, 0, absolute, RecoveryFault::OffsetOverflow);
            return out;
        };
        if bytes.is_empty() {
            out.fail(segment_index, 0, absolute, RecoveryFault::OrderOrChainError);
            return out;
        }
        let mut local = 0_usize;
        let mut local_count = 0_u64;
        let mut local_crc = Crc32::default();
        let mut local_gap = false;
        while local < bytes.len() {
            if finished {
                out.fail(segment_index, local, absolute, RecoveryFault::TrailingDataError);
                return out;
            }
            let view = match scan_frame(&bytes[local..], absolute) {
                Ok(view) => view,
                Err(error) => {
                    out.fail(segment_index, local, absolute, RecoveryFault::Binary(error));
                    out.loss_diagnostics();
                    return out;
                }
            };
            let wide_len = u64::try_from(view.length).expect("frame cap fits u64");
            let Some(end) = absolute.checked_add(wide_len) else {
                out.fail(segment_index, local, absolute, RecoveryFault::OffsetOverflow);
                return out;
            };
            out.framing_good_offset = end;
            let expected_record = out.last_record.map_or(Some(1), |r| r.get().checked_add(1));
            if Some(view.record_no.get()) != expected_record || view.segment_no.get() != segment_number {
                out.fail(segment_index, local, absolute, RecoveryFault::OrderOrChainError);
                return out;
            }
            let has_active = out.model.as_ref().is_some_and(|m| m.timeline.active().is_some());
            let frame = match decode_frame(&view, has_active, &out.env.specs) {
                Ok(frame) => frame,
                Err(error) => {
                    out.fail(segment_index, local, absolute, RecoveryFault::Binary(error));
                    out.loss_diagnostics();
                    return out;
                }
            };
            let position_ok = if segment_index == 0 && local == 0 {
                matches!(frame.value, Record::ArchiveStart(_))
            } else if local == 0 {
                matches!(frame.value, Record::SegmentStart(_))
                    && seal.as_ref().is_some_and(|s| !s.final_segment)
            } else if let Some(link) = &seal {
                link.segment.get() != segment_number
                    || (link.final_segment && matches!(frame.value, Record::ArchiveSeal(_)))
            } else {
                !matches!(frame.value, Record::ArchiveStart(_) | Record::SegmentStart(_))
            };
            if !position_ok {
                out.fail(segment_index, local, absolute, RecoveryFault::OrderOrChainError);
                return out;
            }
            let chain_ok = match &frame.value {
                Record::ArchiveStart(_) => global_count == 0 && absolute == 0,
                Record::SegmentStart(start) => {
                    let valid = out.model.as_ref().zip(seal.as_ref()).is_some_and(|(m, s)| {
                        start.archive == m.start.archive
                            && start.session == m.start.session
                            && start.clock == m.start.clock
                            && start.previous_segment == s.segment
                            && s.segment.get().checked_add(1) == Some(segment_number)
                            && start.previous_seal_record == s.record
                            && start.previous_seal_crc32 == s.checksum
                            && !s.final_segment
                            && local == 0
                    });
                    valid
                }
                Record::SegmentSeal(value) => {
                    value.prefix_frame_count == local_count
                        && u64::try_from(local).ok() == Some(value.prefix_physical_len)
                        && value.prefix_crc32 == local_crc.digest()
                        && Some(value.prior_record) == out.last_record
                        && value.has_gap == local_gap
                }
                Record::ArchiveSeal(value) => {
                    out.loss_diagnostics();
                    let quality_ok = match value.input_quality {
                        InputQuality::NoKnownLoss => !any_gap && out.unresolved_loss_streams.is_empty(),
                        InputQuality::GapsRecorded => any_gap && out.unresolved_loss_streams.is_empty(),
                        InputQuality::Unknown => true,
                    };
                    seal.as_ref().is_some_and(|s| {
                        s.final_segment && s.segment == frame.segment_no && Some(s.record) == out.last_record
                    }) && value.expected_segment_count == segment_number + 1
                        && usize::try_from(value.expected_segment_count).ok() == Some(segments.len())
                        && value.prior_frame_count == global_count
                        && value.total_prefix_physical_bytes == absolute
                        && value.prefix_crc32 == global_crc.digest()
                        && Some(value.prior_record) == out.last_record
                        && quality_ok
                }
                _ => true,
            };
            if !chain_ok {
                out.fail(segment_index, local, absolute, RecoveryFault::OrderOrChainError);
                return out;
            }
            let step = match &frame.value {
                Record::ArchiveStart(start) => {
                    match HealthModel::new(start.clone(), &mut out.env) {
                        Ok(model) => {
                            out.model = Some(model);
                            Ok(StepResult::default())
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => match &mut out.model {
                    Some(model) => model.step(&frame, &mut out.env),
                    None => Err(ModelError::UnknownDefinition),
                },
            };
            let step = match step {
                Ok(step) => step,
                Err(error) => {
                    out.fail(segment_index, local, absolute, RecoveryFault::Semantic(error));
                    out.loss_diagnostics();
                    return out;
                }
            };
            if matches!(frame.value, Record::SegmentStart(_)) {
                seal = None;
            }
            if let Record::SegmentSeal(value) = &frame.value {
                seal = Some(SealLink {
                    segment: frame.segment_no,
                    record: frame.record_no,
                    checksum: view.checksum,
                    final_segment: value.is_final,
                });
            }
            if let Record::ArchiveSeal(value) = &frame.value {
                out.input_quality = Some(value.input_quality);
                finished = true;
            }
            if matches!(frame.value, Record::Gap(_)) {
                local_gap = true;
                any_gap = true;
            }
            let protected = &bytes[local..local + view.length - 4];
            local_crc.update(protected);
            global_crc.update(protected);
            let Some(next_count) = global_count.checked_add(1) else {
                out.fail(segment_index, local, absolute, RecoveryFault::OffsetOverflow);
                return out;
            };
            global_count = next_count;
            local_count = match local_count.checked_add(1) {
                Some(count) => count,
                None => {
                    out.fail(segment_index, local, absolute, RecoveryFault::OffsetOverflow);
                    return out;
                }
            };
            absolute = end;
            local += view.length;
            out.last_good_offset = end;
            out.last_record = Some(frame.record_no);
            out.frames.push(frame);
            out.steps.push(step);
            out.completion = if finished {
                Completion::Complete
            } else if seal.is_some() {
                Completion::SegmentSealedArchiveIncomplete
            } else {
                Completion::ValidPrefixIncomplete
            };
        }
        if segment_index + 1 < segments.len() && seal.is_none() {
            out.fail(segment_index, local, absolute, RecoveryFault::OrderOrChainError);
            return out;
        }
    }
    out.loss_diagnostics();
    out
}

/// Framing-only diagnostic, deliberately not recovery/Complete/applicability.
/// It can describe readable frames after canonical processing was blocked by
/// missing artifact evidence. It does not skip an unreadable frame.
pub fn framing_prefix(segments: &[&[u8]]) -> std::result::Result<u64, Failure> {
    let mut absolute = 0_u64;
    for (segment, bytes) in segments.iter().enumerate() {
        let mut local = 0;
        while local < bytes.len() {
            let view = scan_frame(&bytes[local..], absolute).map_err(|error| Failure {
                segment,
                local_frame_start: local,
                absolute_frame_start: absolute,
                fault: RecoveryFault::Binary(error),
            })?;
            absolute += u64::try_from(view.length).expect("checked cap");
            local += view.length;
        }
    }
    Ok(absolute)
}
