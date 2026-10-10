//! Private composition glue for the synthetic owner-bound capture example.
//! The existing owner, supervisor, sink and reader retain every contract boundary.

mod profile;
pub use profile::{
    ACK, BOOK_ONE, BOOK_THREE, BOOK_TWO, CONNECTED_NS, binding, bootstrap, budget, stamp,
    start_frame,
};

use std::fmt;
use std::path::Path;

use domain::capture_session::{
    AmbiguousEffect, AuthorityError, BoundRecordSink, CaptureSessionAuthority, CloseLeaseReport,
    CloseOwnerRef, CloseState, CommandKind, CommandLease, DispatchReport, FailureCause,
    HeartbeatPolicy, PersistError, PersistErrorKind, QuiescenceReport, RetentionBudget,
    ScopeBinding, SessionDisposition, SessionLifecycle, SessionTurn, UnsettledSummary,
};
use domain::identity::{RecordNo, SegmentNo};
use domain::policy::{RecordingGate, WatermarkKind};
use market_data::{
    AdmissionOutcome, AdmissionReport, DrainReport, PublicWsSupervisor, QueuePolicy, ReceiveStamp,
    SupervisorError, WsSupervisorConfig,
};
use recording::{
    ArchiveStatus, BoundedCaptureProfile, CaptureSessionOwner, DiagnosticCloseState, OwnerError,
    PhysicalReport, SinkFault, SinkFaultKind, StorageWatermarks, WalReader,
};

pub const MAX_OBSERVATIONS: usize = 64;
pub const MAX_EFFECTS: usize = 64;
pub const MAX_PAYLOAD_BYTES: usize = 4096;
pub const MAX_SCRIPT_BYTES: usize = 65_536;
pub const MAX_STEPS: usize = 256;

#[derive(Clone, Copy, Debug)]
pub enum ScriptInput<'a> {
    Connected(ReceiveStamp),
    Text(ReceiveStamp, &'a [u8]),
    Tick(ReceiveStamp),
}

/// Validate the complete immutable observations before an output file is created.
pub fn validate_inputs(inputs: &[ScriptInput<'_>]) -> Result<(), DriverError> {
    if inputs.len() > MAX_OBSERVATIONS {
        return Err(DriverError::new("observation_bound"));
    }
    let mut bytes = 0usize;
    for input in inputs {
        let (receive, payload) = match input {
            ScriptInput::Connected(receive) | ScriptInput::Tick(receive) => (*receive, 0),
            ScriptInput::Text(receive, payload) => (*receive, payload.len()),
        };
        if payload > MAX_PAYLOAD_BYTES {
            return Err(DriverError::new("payload_bound"));
        }
        bytes = bytes
            .checked_add(payload)
            .filter(|bytes| *bytes <= MAX_SCRIPT_BYTES)
            .ok_or_else(|| DriverError::new("script_bytes_bound"))?;
        if stamp(receive.monotonic_ns)? != receive {
            return Err(DriverError::new("non_synthetic_stamp"));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverError {
    pub code: &'static str,
}
impl DriverError {
    pub const fn new(code: &'static str) -> Self {
        Self { code }
    }
}
impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code)
    }
}
impl std::error::Error for DriverError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectEntry {
    pub command: &'static str,
    pub outcome: &'static str,
    pub ambiguous: Option<AmbiguousEffect>,
}

pub struct Driver {
    pub owner: CaptureSessionOwner,
    pub turn: SessionTurn,
    pub sink: BoundRecordSink,
    pub supervisor: PublicWsSupervisor,
    pub effects: Vec<EffectEntry>,
    pub observations: usize,
    pub raw_bytes: usize,
    pub steps: usize,
    authority: CaptureSessionAuthority,
    close_attempts: Vec<(CloseOwnerRef, u8)>,
}

impl Driver {
    pub fn create(path: impl AsRef<Path>, limits: RetentionBudget) -> Result<Self, DriverError> {
        if !(5..=64).contains(&limits.item_cap)
            || !(1..=64).contains(&limits.raw_frame_limit)
            || !(1..=MAX_SCRIPT_BYTES).contains(&limits.raw_byte_limit)
            || !(1..=MAX_PAYLOAD_BYTES).contains(&limits.max_message_bytes)
            || limits.max_message_bytes > limits.raw_byte_limit
        {
            return Err(DriverError::new("invalid_budget"));
        }
        let definitions = bootstrap();
        let (mut owner, mut turn) = CaptureSessionOwner::create_new(
            path,
            &start_frame(),
            BoundedCaptureProfile::new(&definitions),
        )
        .map_err(owner_error)?;
        let stream = binding();
        let scopes = [ScopeBinding {
            stream: stream.id,
            connection: stream.connection_id,
            epoch: stream.tag.connection,
        }];
        let (handle, sink) = owner
            .register_supervisor(&mut turn, &scopes, limits, HeartbeatPolicy::SupervisorV2)
            .map_err(owner_error)?;
        let authority = sink.authority().clone();
        let supervisor = PublicWsSupervisor::new(
            WsSupervisorConfig {
                active_context: profile::active(),
                recording_gate: RecordingGate::Durable,
                segment_no: SegmentNo::new(0),
                next_record_no: RecordNo::new(5).map_err(|_| DriverError::new("fixed_identity"))?,
                queue_policy: QueuePolicy {
                    max_raw_frames_per_stream: limits.raw_frame_limit,
                    max_raw_bytes_per_stream: limits.raw_byte_limit,
                    max_raw_message_bytes: limits.max_message_bytes,
                    max_total_items: limits.item_cap,
                },
                streams: vec![stream],
            },
            handle,
        )
        .map_err(|_| DriverError::new("supervisor_configuration"))?;
        Ok(Self {
            owner,
            turn,
            sink,
            supervisor,
            effects: Vec::with_capacity(MAX_EFFECTS),
            observations: 0,
            raw_bytes: 0,
            steps: 0,
            authority,
            close_attempts: Vec::with_capacity(MAX_EFFECTS),
        })
    }

    fn observe(&mut self, bytes: usize) -> Result<(), DriverError> {
        let observations = self
            .observations
            .checked_add(1)
            .filter(|count| *count <= MAX_OBSERVATIONS)
            .ok_or_else(|| DriverError::new("observation_bound"))?;
        if bytes > MAX_PAYLOAD_BYTES {
            return Err(DriverError::new("payload_bound"));
        }
        let raw_bytes = self
            .raw_bytes
            .checked_add(bytes)
            .filter(|count| *count <= MAX_SCRIPT_BYTES)
            .ok_or_else(|| DriverError::new("script_bytes_bound"))?;
        self.observations = observations;
        self.raw_bytes = raw_bytes;
        Ok(())
    }

    pub fn start(&mut self) -> AdmissionReport {
        self.supervisor.start_commands(&mut self.turn)
    }
    pub fn connect(&mut self, receive: ReceiveStamp) -> Result<AdmissionReport, DriverError> {
        self.observe(0)?;
        let stream = binding();
        Ok(self.supervisor.queue_connected(
            &mut self.turn,
            stream.connection_id,
            stream.tag.connection,
            receive,
        ))
    }
    pub fn text(
        &mut self,
        receive: ReceiveStamp,
        bytes: &[u8],
    ) -> Result<AdmissionReport, DriverError> {
        self.observe(bytes.len())?;
        let stream = binding();
        Ok(self.supervisor.queue_text(
            &mut self.turn,
            stream.connection_id,
            stream.tag.connection,
            receive,
            bytes,
        ))
    }
    pub fn tick(&mut self, receive: ReceiveStamp) -> Result<AdmissionReport, DriverError> {
        self.observe(0)?;
        Ok(self.supervisor.queue_tick(&mut self.turn, receive))
    }
    pub fn apply(&mut self, input: ScriptInput<'_>) -> Result<AdmissionReport, DriverError> {
        match input {
            ScriptInput::Connected(receive) => self.connect(receive),
            ScriptInput::Text(receive, bytes) => self.text(receive, bytes),
            ScriptInput::Tick(receive) => self.tick(receive),
        }
    }
    fn step(&mut self) -> Result<(), DriverError> {
        self.steps = self
            .steps
            .checked_add(1)
            .filter(|count| *count <= MAX_STEPS)
            .ok_or_else(|| DriverError::new("step_bound"))?;
        Ok(())
    }
    pub fn drain(&mut self) -> Result<DrainReport, DriverError> {
        self.step()?;
        Ok(self.supervisor.drain_one(&mut self.turn, &mut self.sink))
    }

    pub fn dispatch(
        &mut self,
        command: CommandLease,
        fail: bool,
    ) -> Result<DispatchReport<&'static str>, DriverError> {
        if self.effects.len() >= MAX_EFFECTS {
            return Err(DriverError::new("effect_bound"));
        }
        let close = command.close_owner().cloned();
        if close.as_ref().is_some_and(|owner| {
            self.close_attempts
                .iter()
                .any(|(existing, attempts)| existing == owner && *attempts >= 2)
        }) {
            // The consumed lease returns to the same Pending owner; this is not a receipt.
            return Err(DriverError::new("close_attempt_bound"));
        }
        let effects = &mut self.effects;
        let close_attempts = &mut self.close_attempts;
        Ok(self.owner.dispatch(&mut self.turn, command, |view| {
            if let Some(owner) = close {
                if let Some((_, count)) = close_attempts
                    .iter_mut()
                    .find(|(existing, _)| *existing == owner)
                {
                    *count = 2; // The preflight permits only the second callback here.
                } else {
                    close_attempts.push((owner, 1));
                }
            }
            let command = match view.kind {
                CommandKind::Connect { .. } => "connect",
                CommandKind::SendText { text } if text == "ping" => "ping",
                CommandKind::SendText { .. } => "subscribe",
                CommandKind::Close => "close",
                CommandKind::ReconnectAfter { .. } => "reconnect_after",
            };
            effects.push(EffectEntry {
                command,
                outcome: if fail { "failed" } else { "succeeded" },
                ambiguous: fail.then_some(AmbiguousEffect::Unknown),
            });
            if fail {
                Err("scripted_transport_error")
            } else {
                Ok(())
            }
        }))
    }

    pub fn local_close(&mut self) -> Result<CloseOwnerRef, DriverError> {
        let stream = binding();
        let epoch = self
            .supervisor
            .snapshot(stream.id)
            .ok_or_else(|| DriverError::new("missing_scope"))?
            .tag
            .connection;
        self.authority
            .mandatory_close(&mut self.turn, stream.id, epoch, None)
            .map_err(|_| DriverError::new("local_close_rejected"))
    }
}

fn owner_error(error: OwnerError) -> DriverError {
    DriverError::new(match error {
        OwnerError::Create(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            "output_exists"
        }
        OwnerError::Create(_) => "output_create_failed",
        OwnerError::Writer(_) => "storage_operation_failed",
        OwnerError::Authority(_) => "owner_authority_rejected",
        OwnerError::InvalidProfile(_) => "invalid_profile",
        OwnerError::CounterExhausted(_) => "owner_counter_exhausted",
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    Nominal,
    HeartbeatPong,
    HeartbeatTimeout,
    TransportError,
    Overflow,
    WriteError,
    ShutdownRefused,
}
impl Scenario {
    pub const ALL: [Self; 7] = [
        Self::Nominal,
        Self::HeartbeatPong,
        Self::HeartbeatTimeout,
        Self::TransportError,
        Self::Overflow,
        Self::WriteError,
        Self::ShutdownRefused,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Nominal => "nominal",
            Self::HeartbeatPong => "heartbeat-pong",
            Self::HeartbeatTimeout => "heartbeat-timeout",
            Self::TransportError => "transport-error",
            Self::Overflow => "overflow",
            Self::WriteError => "write-error",
            Self::ShutdownRefused => "shutdown-refused",
        }
    }
    pub fn parse(value: &str) -> Result<Self, DriverError> {
        Self::ALL
            .into_iter()
            .find(|scenario| scenario.name() == value)
            .ok_or_else(|| DriverError::new("unknown_scenario"))
    }
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Nominal | Self::HeartbeatPong => 0,
            _ => 2,
        }
    }
}

pub struct Summary {
    pub scenario: Scenario,
    pub physical: PhysicalReport,
    pub failed: bool,
    pub lifecycle: SessionLifecycle,
    pub observations: usize,
    pub effects: Vec<EffectEntry>,
    pub outcome: &'static str,
    pub outstanding_close_owners: usize,
    pub unsettled_work: usize,
    pub finalized: bool,
    pub not_ready_observed: bool,
    pub storage_stopped: bool,
    pub storage_error_kind: Option<PersistErrorKind>,
    pub failure_cause: Option<&'static str>,
    pub finalization_invalidated: bool,
    pub close_retry_same_identity: bool,
    pub close_drop_pending: bool,
    pub unsettled: UnsettledSummary,
    pub watermarks: StorageWatermarks,
    pub raw_records: usize,
    pub gap_records: usize,
    pub recorded_loss_count: u64,
    pub trusted_watermarks: StorageWatermarks,
}

impl Summary {
    /// Only fixed labels and library values enter stdout. Environment evidence is external.
    pub fn json(&self) -> String {
        let effects = self
            .effects
            .iter()
            .map(|effect| {
                format!(
                    "{{\"command\":\"{}\",\"outcome\":\"{}\",\"ambiguous_effect\":{}}}",
                    effect.command,
                    effect.outcome,
                    if effect.ambiguous.is_some() {
                        "\"Unknown\""
                    } else {
                        "null"
                    },
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let quality = self
            .physical
            .input_quality
            .map_or_else(|| "null".to_owned(), |quality| format!("\"{quality:?}\""));
        format!(
            concat!(
                "{{\"scenario\":\"{}\",\"synthetic\":true,",
                "\"profile\":\"REC-001F-2-synthetic-unverified-v2\",",
                "\"canonical_status\":\"NotEvaluated\",",
                "\"canonical_applicability\":\"BLOCKED_UNVERIFIED\",\"usable_data\":false,",
                "\"outcome\":\"{}\",\"physical_status\":\"{:?}\",",
                "\"owner_input_quality\":\"Unknown\",\"recovered_input_quality\":{},",
                "\"lifecycle\":\"{:?}\",\"failed\":{},\"storage_stopped\":{},",
                "\"storage_error_kind\":{},\"failure_cause\":{},",
                "\"finalized\":{},\"not_ready_observed\":{},\"finalization_invalidated\":{},",
                "\"close_retry_same_identity\":{},\"close_drop_pending\":{},",
                "\"observations\":{},\"last_record\":{},\"physical_good_offset\":{},",
                "\"outstanding_close_owners\":{},\"unsettled_work\":{},",
                "\"unsettled_record_jobs\":{},\"unsettled_queued\":{},\"unsettled_pending_plans\":{},",
                "\"unsettled_results\":{},\"unsettled_commands\":{},\"abandoned_owners\":{},",
                "\"storage_watermarks\":{},\"trusted_watermarks\":{},",
                "\"raw_records\":{},\"gap_records\":{},\"recorded_loss_count\":{},\"loss_diagnostics\":{},",
                "\"effects\":[{}]}}"
            ),
            self.scenario.name(),
            self.outcome,
            self.physical.status,
            quality,
            self.lifecycle,
            self.failed,
            self.storage_stopped,
            self.storage_error_kind
                .map_or_else(|| "null".to_owned(), |kind| format!("\"{kind:?}\"")),
            self.failure_cause
                .map_or_else(|| "null".to_owned(), |cause| format!("\"{cause}\"")),
            self.finalized,
            self.not_ready_observed,
            self.finalization_invalidated,
            self.close_retry_same_identity,
            self.close_drop_pending,
            self.observations,
            self.physical.last_record.map_or(0, |record| record.get()),
            self.physical.physical_good_offset,
            self.outstanding_close_owners,
            self.unsettled_work,
            self.unsettled.record_jobs,
            self.unsettled.queued,
            self.unsettled.pending_plans,
            self.unsettled.results,
            self.unsettled.commands,
            self.unsettled.abandoned,
            watermarks_json(self.watermarks),
            watermarks_json(self.trusted_watermarks),
            self.raw_records,
            self.gap_records,
            self.recorded_loss_count,
            self.physical.diagnostics.len(),
            effects
        )
    }
}

fn check_disposition(
    disposition: SessionDisposition,
    diagnostic_allowed: bool,
) -> Result<(), DriverError> {
    match disposition {
        SessionDisposition::CaptureEligible(_) => Ok(()),
        SessionDisposition::DiagnosticOnly { .. } if diagnostic_allowed => Ok(()),
        SessionDisposition::DiagnosticOnly { .. } => {
            Err(DriverError::new("diagnostic_disposition"))
        }
        SessionDisposition::Closed(_) => Err(DriverError::new("closed_disposition")),
    }
}

fn dispatch_success(driver: &mut Driver, command: CommandLease) -> Result<(), DriverError> {
    match driver.dispatch(command, false)? {
        DispatchReport::Dispatched | DispatchReport::AlreadySettled => Ok(()),
        DispatchReport::Denied { command, .. } => {
            // Dropping a denied Close lease only returns its exact owner to Pending.
            drop(command);
            Err(DriverError::new("effect_denied"))
        }
        DispatchReport::DispatchFailed { .. } => Err(DriverError::new("effect_failed")),
        DispatchReport::Revoked(_) => Err(DriverError::new("effect_revoked")),
    }
}

fn accept(
    driver: &mut Driver,
    report: AdmissionReport,
    diagnostic_allowed: bool,
) -> Result<AdmissionOutcome, DriverError> {
    check_disposition(report.session_disposition, diagnostic_allowed)?;
    let outcome = report
        .outcome
        .map_err(|_| DriverError::new("admission_rejected"))?;
    for command in report.commands {
        dispatch_success(driver, command)?;
    }
    Ok(outcome)
}

fn drain_all(driver: &mut Driver, diagnostic_allowed: bool) -> Result<(), DriverError> {
    for _ in 0..MAX_STEPS {
        let report = driver.drain()?;
        check_disposition(report.session_disposition, diagnostic_allowed)?;
        let result = report
            .outcome
            .map_err(|_| DriverError::new("drain_rejected"))?;
        let Some(mut result) = result else {
            return Ok(());
        };
        for command in std::mem::take(&mut result.commands) {
            dispatch_success(driver, command)?;
        }
        // The counted result must be released before the next readiness observation.
        drop(result);
    }
    Err(DriverError::new("step_bound"))
}

type Inputs = std::vec::IntoIter<ScriptInput<'static>>;

fn next_input(driver: &mut Driver, inputs: &mut Inputs) -> Result<AdmissionReport, DriverError> {
    driver.apply(
        inputs
            .next()
            .ok_or_else(|| DriverError::new("missing_script_input"))?,
    )
}

fn initialize(driver: &mut Driver, inputs: &mut Inputs) -> Result<(), DriverError> {
    let report = driver.start();
    accept(driver, report, false)?;
    let report = next_input(driver, inputs)?;
    accept(driver, report, false)?;
    drain_all(driver, false)?;
    let report = next_input(driver, inputs)?;
    accept(driver, report, false)?;
    drain_all(driver, false)
}

fn nominal_inputs(driver: &mut Driver, inputs: &mut Inputs) -> Result<(), DriverError> {
    for _ in 0..3 {
        let report = next_input(driver, inputs)?;
        accept(driver, report, false)?;
    }
    drain_all(driver, false)
}

fn reclaim(driver: &mut Driver, owner: CloseOwnerRef) -> Result<CommandLease, DriverError> {
    driver.step()?;
    match driver.owner.reclaim_close(&mut driver.turn, owner) {
        CloseLeaseReport::Leased(lease) => lease
            .into_command()
            .map_err(|_| DriverError::new("close_command_rejected")),
        CloseLeaseReport::Rejected(AuthorityError::CloseNotReady) => {
            Err(DriverError::new("close_not_ready"))
        }
        _ => Err(DriverError::new("close_reclaim_rejected")),
    }
}

fn settle_pending(driver: &mut Driver) -> Result<(), DriverError> {
    let snapshot = driver.owner.outstanding_close_owners();
    for close in snapshot
        .iter()
        .filter(|close| close.state != CloseState::Settled)
    {
        let command = reclaim(driver, close.owner.clone())?;
        dispatch_success(driver, command)?;
    }
    Ok(())
}

fn healthy_finish(driver: &mut Driver) -> Result<bool, DriverError> {
    // A local stop is an owner-issued mandatory Close, never a received Disconnected.
    let owner = driver.local_close()?;
    let settled = driver
        .authority
        .close_state(owner.stream(), owner.epoch())
        .map_err(|_| DriverError::new("close_state_rejected"))?
        == CloseState::Settled;
    let command = if settled {
        None
    } else {
        Some(reclaim(driver, owner)?)
    };
    let ticket = driver
        .owner
        .begin_finalization(&mut driver.turn)
        .map_err(owner_error)?;
    driver.step()?;
    let (not_ready, mut proof) = match driver.supervisor.quiesce(&mut driver.turn, &ticket) {
        QuiescenceReport::Ready(proof) if command.is_none() => (false, proof),
        QuiescenceReport::NotReady(_) => {
            let command = command.ok_or_else(|| DriverError::new("unexpected_unsettled_work"))?;
            dispatch_success(driver, command)?;
            driver.step()?;
            let QuiescenceReport::Ready(proof) =
                driver.supervisor.quiesce(&mut driver.turn, &ticket)
            else {
                return Err(DriverError::new("finalization_not_ready"));
            };
            (true, proof)
        }
        _ => return Err(DriverError::new("finalization_not_ready")),
    };
    let receipt = driver
        .owner
        .finalize(&mut driver.turn, &mut proof)
        .map_err(owner_error)?;
    if receipt.input_quality() != domain::record::InputQuality::Unknown {
        return Err(DriverError::new("unexpected_owner_quality"));
    }
    if driver.owner.finalize(&mut driver.turn, &mut proof).is_ok() {
        return Err(DriverError::new("duplicate_finalization_accepted"));
    }
    Ok(not_ready)
}

fn diagnostic_finish(driver: &mut Driver) -> Result<(), DriverError> {
    // StorageStopped prohibits writes but still permits this owner's transport Close.
    driver.local_close()?;
    settle_pending(driver)?;
    driver.step()?;
    let report = driver.owner.close_diagnostic(&mut driver.turn);
    match report.outcome {
        Ok(DiagnosticCloseState::Closed) if report.physical_report.descriptor_closed => Ok(()),
        _ => Err(DriverError::new("diagnostic_close_not_closed")),
    }
}

pub fn readback(path: impl AsRef<Path>) -> Result<PhysicalReport, DriverError> {
    let mut reader = WalReader::open(path).map_err(|_| DriverError::new("reader_open_failed"))?;
    for _ in 0..256 {
        match reader.next_record() {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(reader.report().clone()),
            Err(_) => return Ok(reader.report().clone()),
        }
    }
    Err(DriverError::new("readback_bound"))
}

pub fn run_scenario(path: impl AsRef<Path>, scenario: Scenario) -> Result<Summary, DriverError> {
    let path = path.as_ref();
    let inputs = scenario_inputs(scenario)?;
    validate_inputs(&inputs)?;
    let input_count = inputs.len();
    let mut inputs = inputs.into_iter();
    let mut limits = budget();
    if scenario == Scenario::Overflow {
        limits.item_cap = 5;
    }
    let mut driver = Driver::create(path, limits)?;
    initialize(&mut driver, &mut inputs)?;
    let mut not_ready = false;
    let mut finalized = false;
    let mut invalidated = false;
    let mut retry_same = false;
    let mut drop_pending = false;
    let outcome = match scenario {
        Scenario::Nominal => {
            nominal_inputs(&mut driver, &mut inputs)?;
            not_ready = healthy_finish(&mut driver)?;
            finalized = true;
            "complete"
        }
        Scenario::HeartbeatPong | Scenario::HeartbeatTimeout => {
            nominal_inputs(&mut driver, &mut inputs)?;
            let report = next_input(&mut driver, &mut inputs)?;
            accept(&mut driver, report, false)?;
            drain_all(&mut driver, false)?;
            if scenario == Scenario::HeartbeatPong {
                let report = next_input(&mut driver, &mut inputs)?;
                accept(&mut driver, report, false)?;
                drain_all(&mut driver, false)?;
                let report = next_input(&mut driver, &mut inputs)?;
                accept(&mut driver, report, false)?;
                drain_all(&mut driver, false)?;
                not_ready = healthy_finish(&mut driver)?;
                finalized = true;
                "pong_recorded"
            } else {
                let report = next_input(&mut driver, &mut inputs)?;
                accept(&mut driver, report, false)?;
                drain_all(&mut driver, false)?;
                // Timeout can generate lawful restoration work; the supervisor drains it.
                not_ready = healthy_finish(&mut driver)?;
                finalized = true;
                "heartbeat_timeout"
            }
        }
        Scenario::TransportError => {
            nominal_inputs(&mut driver, &mut inputs)?;
            let report = next_input(&mut driver, &mut inputs)?;
            accept(&mut driver, report, false)?;
            let report = driver.drain()?;
            check_disposition(report.session_disposition, false)?;
            let mut result = report
                .outcome
                .map_err(|_| DriverError::new("drain_rejected"))?
                .ok_or_else(|| DriverError::new("missing_ping_result"))?;
            let commands = std::mem::take(&mut result.commands).into_vec();
            if commands.len() != 1
                || !matches!(commands[0].kind(),
                CommandKind::SendText { text } if text == "ping")
            {
                return Err(DriverError::new("expected_ping_lease"));
            }
            for command in commands {
                match driver.dispatch(command, true)? {
                    DispatchReport::DispatchFailed {
                        effect: AmbiguousEffect::Unknown,
                        ..
                    } => {}
                    _ => return Err(DriverError::new("expected_ambiguous_ping_error")),
                }
            }
            drop(result);
            let owner = driver.local_close()?;
            let dropped = reclaim(&mut driver, owner.clone())?;
            if dropped.close_owner() != Some(&owner) {
                return Err(DriverError::new("close_identity_changed"));
            }
            drop(dropped);
            drop_pending = driver
                .owner
                .outstanding_close_owners()
                .iter()
                .any(|close| close.owner == owner && close.state == CloseState::Pending);
            let command = reclaim(&mut driver, owner.clone())?;
            match driver.dispatch(command, true)? {
                DispatchReport::DispatchFailed {
                    effect: AmbiguousEffect::Unknown,
                    ..
                } => {}
                _ => return Err(DriverError::new("expected_ambiguous_close_error")),
            }
            let command = reclaim(&mut driver, owner.clone())?;
            retry_same = command.close_owner() == Some(&owner);
            if !retry_same || !drop_pending {
                return Err(DriverError::new("close_identity_changed"));
            }
            dispatch_success(&mut driver, command)?;
            not_ready = healthy_finish(&mut driver)?;
            finalized = true;
            "transport_ambiguous_settled"
        }
        Scenario::Overflow => {
            // Actual scripted observations exhaust the three available work items.
            for _ in 0..3 {
                let report = next_input(&mut driver, &mut inputs)?;
                accept(&mut driver, report, false)?;
            }
            let report = next_input(&mut driver, &mut inputs)?;
            check_disposition(report.session_disposition, true)?;
            if !matches!(
                report.outcome,
                Err(SupervisorError::QueueExhausted { .. })
                    | Err(SupervisorError::Authority(AuthorityError::WorkExhausted))
            ) || report.failure.is_none()
                || report.close_owner.is_none()
            {
                return Err(DriverError::new("expected_terminal_overflow"));
            }
            for command in report.commands {
                dispatch_success(&mut driver, command)?;
            }
            drain_all(&mut driver, true)?;
            diagnostic_finish(&mut driver)?;
            "terminal_work_exhaustion"
        }
        Scenario::WriteError | Scenario::ShutdownRefused => {
            let next = driver
                .owner
                .watermarks()
                .written
                .ok_or_else(|| DriverError::new("missing_written_prefix"))?
                .checked_next()
                .map_err(|_| DriverError::new("record_bound"))?;
            let report = next_input(&mut driver, &mut inputs)?;
            accept(&mut driver, report, false)?;
            let ticket = if scenario == Scenario::ShutdownRefused {
                Some(
                    driver
                        .owner
                        .begin_finalization(&mut driver.turn)
                        .map_err(owner_error)?,
                )
            } else {
                None
            };
            if let Some(ticket) = &ticket {
                driver.step()?;
                not_ready = matches!(
                    driver.supervisor.quiesce(&mut driver.turn, ticket),
                    QuiescenceReport::NotReady(_)
                );
                if !not_ready {
                    return Err(DriverError::new("expected_not_ready"));
                }
            }
            driver
                .owner
                .set_sink_fault(
                    &mut driver.turn,
                    Some(SinkFault {
                        at: next,
                        kind: SinkFaultKind::BeforeWrite(PersistError::typed(
                            PersistErrorKind::Io,
                            "synthetic before-write failure",
                        )),
                    }),
                )
                .map_err(owner_error)?;
            let report = driver.drain()?;
            check_disposition(report.session_disposition, true)?;
            if report.outcome.is_ok() || driver.owner.session_status().storage_stopped.is_none() {
                return Err(DriverError::new("expected_storage_stop"));
            }
            let calls = driver.owner.sink_persist_calls();
            let denied = driver.drain()?;
            check_disposition(denied.session_disposition, true)?;
            if denied.outcome.is_ok() || driver.owner.sink_persist_calls() != calls {
                return Err(DriverError::new("write_retry_after_stop"));
            }
            if let Some(ticket) = &ticket {
                driver.step()?;
                invalidated = matches!(
                    driver.supervisor.quiesce(&mut driver.turn, ticket),
                    QuiescenceReport::FinalizationInvalidated(_)
                );
                if !invalidated {
                    return Err(DriverError::new("expected_finalization_invalidated"));
                }
            } else if driver.owner.begin_finalization(&mut driver.turn).is_ok() {
                return Err(DriverError::new("storage_stop_finalization_accepted"));
            }
            diagnostic_finish(&mut driver)?;
            if scenario == Scenario::ShutdownRefused {
                "finalization_invalidated"
            } else {
                "storage_stopped"
            }
        }
    };
    if inputs.next().is_some() || driver.observations != input_count {
        return Err(DriverError::new("script_consumption_mismatch"));
    }
    let physical = readback(path)?;
    let status = driver.owner.session_status();
    if physical.failure.is_some() {
        return Err(DriverError::new("capture_readback_failed"));
    }
    if finalized
        && (physical.status != ArchiveStatus::Complete
            || physical.input_quality != Some(domain::record::InputQuality::Unknown)
            || status.lifecycle != SessionLifecycle::Finalized
            || status.failed
            || status.storage_stopped.is_some())
    {
        return Err(DriverError::new("finalized_readback_mismatch"));
    }
    if !finalized
        && (physical.status != ArchiveStatus::ValidPrefixIncomplete
            || physical.input_quality.is_some()
            || status.lifecycle != SessionLifecycle::DiagnosticClosed)
    {
        return Err(DriverError::new("diagnostic_readback_mismatch"));
    }
    let outstanding_close_owners = driver
        .owner
        .outstanding_close_owners()
        .iter()
        .filter(|close| close.state != CloseState::Settled)
        .count();
    let unsettled_work = driver.authority.ownership_report().work_used;
    let unsettled = driver.authority.unsettled_summary();
    let watermarks = driver.owner.watermarks();
    let trusted_watermarks = StorageWatermarks {
        accepted: driver.authority.trusted_watermark(WatermarkKind::Accepted),
        appended: driver.authority.trusted_watermark(WatermarkKind::Appended),
        written: driver.authority.trusted_watermark(WatermarkKind::Written),
        flushed: driver.authority.trusted_watermark(WatermarkKind::Flushed),
        durable: driver.authority.trusted_watermark(WatermarkKind::Durable),
    };
    let (raw_records, gap_records, recorded_loss_count) = readback_counts(path)?;
    Ok(Summary {
        scenario,
        physical,
        failed: status.failed,
        lifecycle: status.lifecycle,
        observations: driver.observations,
        effects: driver.effects,
        outcome,
        outstanding_close_owners,
        unsettled_work,
        finalized,
        not_ready_observed: not_ready,
        storage_stopped: status.storage_stopped.is_some(),
        storage_error_kind: status.storage_stopped.map(|error| error.kind),
        failure_cause: status.first_failure.map(|failure| match failure.cause {
            FailureCause::QueueOverflow => "QueueOverflow",
            FailureCause::CaptureAttemptExhausted => "CaptureAttemptExhausted",
            FailureCause::CounterExhausted(_) => "CounterExhausted",
            FailureCause::TimeOverflow => "TimeOverflow",
            FailureCause::StorageFailure => "StorageFailure",
            FailureCause::ReceiptMismatch => "ReceiptMismatch",
            FailureCause::WeakGate => "WeakGate",
            FailureCause::OrderingFailure => "OrderingFailure",
        }),
        finalization_invalidated: invalidated,
        close_retry_same_identity: retry_same,
        close_drop_pending: drop_pending,
        unsettled,
        watermarks,
        raw_records,
        gap_records,
        recorded_loss_count,
        trusted_watermarks,
    })
}

fn watermarks_json(marks: StorageWatermarks) -> String {
    fn value(record: Option<RecordNo>) -> String {
        record.map_or_else(|| "null".to_owned(), |record| record.get().to_string())
    }
    format!(
        "{{\"accepted\":{},\"appended\":{},\"written\":{},\"flushed\":{},\"durable\":{}}}",
        value(marks.accepted),
        value(marks.appended),
        value(marks.written),
        value(marks.flushed),
        value(marks.durable)
    )
}

fn scenario_inputs(scenario: Scenario) -> Result<Vec<ScriptInput<'static>>, DriverError> {
    let mut inputs = vec![
        ScriptInput::Connected(stamp(CONNECTED_NS)?),
        ScriptInput::Text(stamp(200)?, ACK),
    ];
    match scenario {
        Scenario::WriteError | Scenario::ShutdownRefused => {
            inputs.push(ScriptInput::Text(stamp(300)?, BOOK_ONE))
        }
        Scenario::Overflow => {
            for at in [300, 301, 302] {
                inputs.push(ScriptInput::Connected(stamp(at)?));
            }
            inputs.push(ScriptInput::Text(stamp(400)?, BOOK_ONE));
        }
        _ => {
            for (at, bytes) in [(300, BOOK_ONE), (400, BOOK_TWO), (500, BOOK_THREE)] {
                inputs.push(ScriptInput::Text(stamp(at)?, bytes));
            }
            if scenario != Scenario::Nominal {
                let ping = CONNECTED_NS
                    .checked_add(HeartbeatPolicy::SupervisorV2.ping_interval_ns())
                    .ok_or_else(|| DriverError::new("stamp_overflow"))?;
                inputs.push(ScriptInput::Tick(stamp(ping)?));
                if matches!(
                    scenario,
                    Scenario::HeartbeatPong | Scenario::HeartbeatTimeout
                ) {
                    let timeout = ping
                        .checked_add(HeartbeatPolicy::SupervisorV2.pong_timeout_ns())
                        .ok_or_else(|| DriverError::new("stamp_overflow"))?;
                    if scenario == Scenario::HeartbeatPong {
                        inputs.push(ScriptInput::Text(
                            stamp(
                                timeout
                                    .checked_sub(1)
                                    .ok_or_else(|| DriverError::new("stamp_overflow"))?,
                            )?,
                            b"pong",
                        ));
                    }
                    inputs.push(ScriptInput::Tick(stamp(timeout)?));
                }
            }
        }
    }
    Ok(inputs)
}

fn readback_counts(path: &Path) -> Result<(usize, usize, u64), DriverError> {
    use domain::record::{GapScope, Record};
    let mut reader = WalReader::open(path).map_err(|_| DriverError::new("reader_open_failed"))?;
    let mut raw = 0usize;
    let mut gaps = 0usize;
    let mut lost = 0u64;
    for _ in 0..256 {
        match reader
            .next_record()
            .map_err(|_| DriverError::new("capture_readback_failed"))?
        {
            None => return Ok((raw, gaps, lost)),
            Some(frame) => match frame.value {
                Record::RawInput(_) => {
                    raw = raw
                        .checked_add(1)
                        .ok_or_else(|| DriverError::new("readback_bound"))?
                }
                Record::Gap(gap) => {
                    gaps = gaps
                        .checked_add(1)
                        .ok_or_else(|| DriverError::new("readback_bound"))?;
                    if let GapScope::ExplicitTargets(targets) = gap.scope {
                        for target in targets {
                            if let Some(count) = target.loss_count {
                                lost = lost
                                    .checked_add(count)
                                    .ok_or_else(|| DriverError::new("loss_count_bound"))?;
                            }
                        }
                    }
                }
                _ => {}
            },
        }
    }
    Err(DriverError::new("readback_bound"))
}
