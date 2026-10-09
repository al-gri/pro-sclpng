//! App composition only: accepted reader, decoder and health reducer own semantics.

pub mod profile;

use std::io::{self, Write};
use std::path::Path;

use domain::event::{ClockScope, MonotonicSample};
use domain::identity::{MonotonicNs, RecordNo};
use domain::record::{Control, Record, RecordFrame, RecordingHealth};
use market_data::{
    BitgetMessage, BookFrameObservation, Category, DataHealthReducer, DecodeError,
    HealthObservation, RecordedHealthObservation, Topic, decode_message_with_limits,
};
use recording::{ArchiveStatus, PhysicalReport, WalReader};

struct Projection {
    reducer: DataHealthReducer,
    scope: ClockScope,
    sample_ns: u64,
    instrument: bool,
    inactive_instrument: bool,
    stream: bool,
    config: bool,
    failed_latched: bool,
    recording: RecordingHealth,
}

impl Projection {
    fn new() -> Result<Self, market_data::HealthError> {
        let scope = profile::clock_scope();
        Ok(Self {
            reducer: DataHealthReducer::new(
                scope,
                profile::health_policy(),
                profile::durability_mode(),
            )?,
            scope,
            sample_ns: 0,
            instrument: false,
            inactive_instrument: false,
            stream: false,
            config: false,
            failed_latched: false,
            recording: RecordingHealth::Unknown,
        })
    }

    fn ready(&self) -> bool {
        self.instrument && self.stream && self.config
    }

    fn observe(
        &mut self,
        frame: &RecordFrame,
        out: &mut impl Write,
    ) -> io::Result<Result<HealthObservation, &'static str>> {
        let observation = match &frame.value {
            Record::ArchiveStart(value) => {
                if value.archive != profile::archive_id()
                    || value.session != profile::session_id()
                    || value.clock != profile::clock_id()
                    || value.mode != profile::durability_mode()
                    || value.previous_archive.is_some()
                {
                    return Ok(Err("ProfileArchiveMismatch"));
                }
                HealthObservation::Noop
            }
            Record::InstrumentSpec(value) => {
                if !self.instrument && *value == profile::instrument_spec() {
                    self.instrument = true;
                } else if self.instrument
                    && !self.config
                    && !self.inactive_instrument
                    && *value == profile::inactive_instrument_spec()
                {
                    self.inactive_instrument = true;
                } else {
                    return Ok(Err("InstrumentDefinitionMismatch"));
                }
                HealthObservation::Noop
            }
            Record::StreamDefinition(value) => {
                if !self.instrument || self.stream || *value != profile::stream_definition() {
                    return Ok(Err("StreamDefinitionMismatch"));
                }
                self.stream = true;
                HealthObservation::RegisterStream(value.binding.clone())
            }
            Record::ConfigDefinition(value) => {
                if self.config {
                    return Ok(Err("UnsupportedLaterConfigDefinition"));
                }
                if !self.instrument || !self.stream || *value != profile::config_definition() {
                    return Ok(Err("InitialConfigMismatch"));
                }
                self.config = true;
                HealthObservation::Noop
            }
            _ if !self.ready() => return Ok(Err("MissingInitialDefinitions")),
            Record::RawInput(raw) => {
                let decoded = match decode_message_with_limits(&raw.bytes, profile::decode_limits())
                {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        writeln!(out, "semantic_error={error:?}")?;
                        return Ok(Err(match error {
                            DecodeError::UnsupportedCategory { .. }
                            | DecodeError::UnsupportedTopic { .. }
                            | DecodeError::UnsupportedProfile { .. } => "UnsupportedPayload",
                            DecodeError::UnexpectedDataCount { .. } => "NonHomogeneousBookFrame",
                            _ => "MalformedPayload",
                        }));
                    }
                };
                let BitgetMessage::Books50(book) = decoded else {
                    return Ok(Err("UnsupportedPayload"));
                };
                let binding = profile::stream_definition().binding;
                if raw.stream != binding.id
                    || book.category != Category::UsdtFutures
                    || book.symbol != binding.spec.instrument.native_symbol.as_str()
                    || book.topic != Topic::Books50
                {
                    return Ok(Err("RawIdentityMismatch"));
                }
                writeln!(out, "lexical={book:?}")?;
                let raw_bytes = match u32::try_from(raw.bytes.len()) {
                    Ok(bytes) => bytes,
                    Err(_) => return Ok(Err("RawByteCountOverflow")),
                };
                HealthObservation::BookFrame(BookFrameObservation {
                    stream: raw.stream,
                    tag: raw.tag,
                    raw_bytes,
                    // Pinned cardinality: one candidate per homogeneous decoded frame.
                    candidate_outputs: 1,
                    frame: book,
                })
            }
            Record::Control(control) => match &control.value {
                Control::Timer { stream, .. } => HealthObservation::Timer { stream: *stream },
                Control::Transport {
                    connection,
                    epoch,
                    value,
                } => HealthObservation::Transport {
                    connection: *connection,
                    epoch: *epoch,
                    value: *value,
                },
                Control::EpochAdvance { change, .. } => {
                    HealthObservation::EpochAdvance(change.clone())
                }
                Control::Recording(evidence) => {
                    self.failed_latched |= evidence.health == RecordingHealth::Failed;
                    self.recording = if self.failed_latched {
                        RecordingHealth::Failed
                    } else {
                        evidence.health
                    };
                    writeln!(
                        out,
                        "recording_observed={evidence:?} recording_effective={:?} failed_latched={}",
                        self.recording, self.failed_latched
                    )?;
                    // No RecordingEvidence observation exists in the accepted reducer.
                    HealthObservation::Noop
                }
                Control::SpecActivate { .. } => return Ok(Err("UnsupportedSpecActivate")),
                Control::Verification(_) => return Ok(Err("UnsupportedVerification")),
                Control::Warmup(_) => return Ok(Err("UnsupportedWarmup")),
                Control::Freshness(_) => return Ok(Err("UnsupportedFreshness")),
            },
            Record::Gap(gap) => HealthObservation::Gap {
                scope: gap.scope.clone(),
                reason: gap.reason,
            },
            Record::SegmentSeal(_) | Record::ArchiveSeal(_) => HealthObservation::Noop,
            Record::SegmentStart(_) => return Ok(Err("UnsupportedMultiSegment")),
        };
        Ok(Ok(observation))
    }
}

fn physical(out: &mut impl Write, report: &PhysicalReport) -> io::Result<()> {
    writeln!(out, "physical_status={:?}", report.status)?;
    writeln!(out, "input_quality={:?}", report.input_quality)?;
    writeln!(out, "last_good_offset={}", report.physical_good_offset)?;
    writeln!(out, "framing_good_offset={}", report.framing_good_offset)?;
    writeln!(out, "last_record={:?}", report.last_record)?;
    writeln!(out, "physical_frontier={:?}", report.last_record)?;
    writeln!(out, "failure={:?}", report.failure)?;
    writeln!(out, "recovery_diagnostics={:?}", report.diagnostics)
}

fn pre_admission_failure(out: &mut impl Write, reason: &str) -> io::Result<bool> {
    writeln!(out, "physical_status=NotScanned")?;
    writeln!(out, "input_quality=None")?;
    writeln!(out, "last_good_offset=0")?;
    writeln!(out, "framing_good_offset=0")?;
    writeln!(out, "last_record=None")?;
    writeln!(out, "physical_frontier=None")?;
    writeln!(out, "failure=None")?;
    writeln!(out, "physical_scan=NotStarted")?;
    writeln!(out, "admission_failure={reason}")?;
    writeln!(out, "semantic_frontier=None")?;
    writeln!(out, "semantic_block_record=None")?;
    writeln!(out, "semantic_block_reason=Some({reason:?})")?;
    writeln!(out, "diagnostic_prefix_complete=false")?;
    Ok(false)
}

pub fn run(path: &Path, out: &mut impl Write) -> io::Result<bool> {
    writeln!(out, "diagnostic_format={}", profile::NAME)?;
    writeln!(out, "canonical_status=NotEvaluated")?;
    writeln!(out, "canonical_applicability=BLOCKED_UNVERIFIED")?;
    writeln!(out, "usable_data=false")?;
    writeln!(out, "artifact_applicability=UnresolvedSyntheticDescriptors")?;
    writeln!(out, "timer_authorization=NotReconstructed")?;
    writeln!(out, "storage_fence=NotVerified")?;
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return pre_admission_failure(out, &format!("MetadataIo({:?})", error.kind()));
        }
    };
    if !metadata.is_file() {
        return pre_admission_failure(out, "NotRegularFile");
    }
    if metadata.len() > profile::MAX_ARCHIVE_BYTES {
        return pre_admission_failure(out, "ArchiveByteCap");
    }
    let mut reader = match WalReader::open(path) {
        Ok(reader) => reader,
        Err(error) => return pre_admission_failure(out, &format!("OpenIo({:?})", error.kind())),
    };
    let mut projection = match Projection::new() {
        Ok(projection) => projection,
        Err(error) => return pre_admission_failure(out, &format!("PinnedPolicy({error:?})")),
    };
    let mut count = 0usize;
    let mut declared = 0usize;
    let mut block: Option<(RecordNo, &'static str)> = None;
    let mut scan_stop = None;
    while let Ok(Some(frame)) = reader.next_record() {
        // One bounded extra reader observation distinguishes exact-cap EOF from exhaustion.
        count = match count.checked_add(1) {
            Some(count) => count,
            None => {
                block.get_or_insert((frame.record_no, "RecordCountOverflow"));
                break;
            }
        };
        if count > profile::MAX_RECORDS {
            scan_stop = Some("RecordCap");
            block.get_or_insert((frame.record_no, "RecordCap"));
            break;
        }
        if reader.report().framing_good_offset > profile::MAX_ARCHIVE_BYTES {
            scan_stop = Some("ArchiveByteCap");
            block.get_or_insert((frame.record_no, "ArchiveByteCap"));
            break;
        }
        if matches!(frame.value, Record::StreamDefinition(_)) {
            declared = match declared.checked_add(1) {
                Some(declared) => declared,
                None => {
                    block.get_or_insert((frame.record_no, "StreamCountOverflow"));
                    break;
                }
            };
            if declared > profile::MAX_STREAMS {
                scan_stop = Some("DeclaredStreamCap");
                block.get_or_insert((frame.record_no, "DeclaredStreamCap"));
                break;
            }
        }
        if block.is_some() {
            writeln!(
                out,
                "record={} kind={:?} semantic=NotApplied",
                frame.record_no.get(),
                frame.value.kind()
            )?;
            continue;
        }
        let sample_ns = frame
            .value
            .context()
            .map_or(projection.sample_ns, |c| c.monotonic_ns.get());
        writeln!(
            out,
            "record={} kind={:?} sample_ns={sample_ns}",
            frame.record_no.get(),
            frame.value.kind()
        )?;
        if let Some(context) = frame.value.context() {
            writeln!(out, "recorded_context={context:?}")?;
        }
        match &frame.value {
            Record::ArchiveStart(value) => writeln!(out, "structural={value:?}")?,
            Record::InstrumentSpec(value) => writeln!(out, "structural={value:?}")?,
            Record::StreamDefinition(value) => writeln!(out, "structural={value:?}")?,
            Record::ConfigDefinition(value) => writeln!(out, "structural={value:?}")?,
            Record::Gap(value) => writeln!(out, "gap={value:?}")?,
            Record::SegmentSeal(value) => writeln!(out, "structural={value:?}")?,
            Record::ArchiveSeal(value) => writeln!(out, "structural={value:?}")?,
            _ => {}
        }
        if let Record::Control(control) = &frame.value {
            writeln!(out, "control={:?}", control.value)?;
        }
        let observation = match projection.observe(&frame, out)? {
            Ok(observation) => observation,
            Err(reason) => {
                block = Some((frame.record_no, reason));
                continue;
            }
        };
        let sample = MonotonicSample {
            scope: projection.scope,
            ns: MonotonicNs::new(sample_ns),
        };
        match projection.reducer.step(RecordedHealthObservation {
            record: frame.record_no,
            sample,
            value: observation,
        }) {
            Ok(result) => {
                projection.sample_ns = sample_ns;
                writeln!(out, "health={result:?}")?;
                writeln!(out, "health_snapshot={:?}", projection.reducer.snapshot())?;
            }
            Err(error) => {
                writeln!(out, "semantic_error={error:?}")?;
                block = Some((frame.record_no, "HealthObservationRejected"));
            }
        }
    }
    physical(out, reader.report())?;
    writeln!(out, "physical_scan_stop={scan_stop:?}")?;
    let complete =
        reader.report().status == ArchiveStatus::Complete && block.is_none() && projection.ready();
    writeln!(
        out,
        "semantic_frontier={:?}",
        projection.reducer.last_record()
    )?;
    writeln!(
        out,
        "semantic_block_record={:?}",
        block.map(|(record, _)| record)
    )?;
    writeln!(
        out,
        "semantic_block_reason={:?}",
        block.map(|(_, reason)| reason)
    )?;
    writeln!(out, "diagnostic_prefix_complete={complete}")?;
    writeln!(
        out,
        "recording_effective={:?} failed_latched={}",
        projection.recording, projection.failed_latched
    )?;
    let snapshot = projection.reducer.snapshot();
    for stream in &snapshot.streams {
        writeln!(
            out,
            "stream={} usable_data={}",
            stream.binding.id.get(),
            projection.reducer.usable_data(stream.binding.id)
        )?;
    }
    writeln!(out, "final_health={snapshot:?}")?;
    Ok(complete)
}
