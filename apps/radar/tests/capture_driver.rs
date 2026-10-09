//! Application composition checks on genuine filesystem owner capabilities.
#![forbid(unsafe_code)]

#[path = "../src/capture_driver/mod.rs"]
mod capture_driver;
#[path = "../src/capture_driver/timer_stage_checks.rs"]
mod timer_stage_checks;

use capture_driver::{
    ACK, BOOK_ONE, BOOK_THREE, BOOK_TWO, Driver, Scenario, binding, budget, stamp,
};
use domain::capture_session::{
    AmbiguousEffect, AuthorityError, CloseLeaseReport, CloseState, CommandKind, CommandLease,
    DispatchReport, QuiescenceReport, RetentionBudget, SessionDisposition, SessionLifecycle,
};
use domain::identity::RecordNo;
use domain::record::{Control, InputQuality, Record, RecordFrame, Transport};
use market_data::{AdmissionOutcome, AdmissionReport, DrainResult, SupervisorError};
use recording::{ArchiveStatus, CodecErrorKind, FailureKind, PhysicalReport, WalReader};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);
const CONNECTED: u64 = 100;
const PING: u64 = CONNECTED + 30_000_000_000;
const TIMEOUT: u64 = PING + 15_000_000_000;

struct TempWal(PathBuf);
impl TempWal {
    fn new() -> Self {
        let serial = NEXT_PATH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("finite test path counter");
        Self(std::env::temp_dir().join(format!("rec-f2-app-{}-{serial}.wal", std::process::id())))
    }
}
impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn eligible(disposition: SessionDisposition) {
    assert!(matches!(
        disposition,
        SessionDisposition::CaptureEligible(_)
    ));
}

fn admission(report: AdmissionReport) -> Vec<CommandLease> {
    eligible(report.session_disposition);
    assert_eq!(report.outcome, Ok(AdmissionOutcome::Admitted));
    assert!(report.failure.is_none());
    assert!(report.close_owner.is_none());
    report.commands.into_iter().collect()
}

fn dispatch(driver: &mut Driver, commands: impl IntoIterator<Item = CommandLease>) {
    for command in commands {
        assert!(matches!(
            driver.dispatch(command, false).unwrap(),
            DispatchReport::Dispatched
        ));
    }
}

fn drain(driver: &mut Driver) -> DrainResult {
    let report = driver.drain().unwrap();
    eligible(report.session_disposition);
    report.outcome.unwrap().expect("one admitted observation")
}

fn settle_result(driver: &mut Driver, mut result: DrainResult) {
    let commands = std::mem::take(&mut result.commands);
    dispatch(driver, commands);
    drop(result);
}

fn prepared(path: &Path, limits: RetentionBudget) -> Driver {
    let mut driver = Driver::create(path, limits).unwrap();
    let report = driver.supervisor.start_commands(&mut driver.turn);
    dispatch(&mut driver, admission(report));
    let b = binding();
    let report = driver.supervisor.queue_connected(
        &mut driver.turn,
        b.connection_id,
        b.tag.connection,
        stamp(CONNECTED).unwrap(),
    );
    assert!(admission(report).is_empty());
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    text(&mut driver, ACK, 101);
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    driver
}

fn text(driver: &mut Driver, bytes: &[u8], time: u64) {
    let b = binding();
    let report = driver.supervisor.queue_text(
        &mut driver.turn,
        b.connection_id,
        b.tag.connection,
        stamp(time).unwrap(),
        bytes,
    );
    assert!(admission(report).is_empty());
}

fn tick(driver: &mut Driver, time: u64) -> AdmissionReport {
    driver
        .supervisor
        .queue_tick(&mut driver.turn, stamp(time).unwrap())
}

fn ping(driver: &mut Driver) -> CommandLease {
    assert!(admission(tick(driver, PING)).is_empty());
    let mut result = drain(driver);
    assert_eq!(result.records.len(), 1);
    let mut commands = std::mem::take(&mut result.commands).into_iter();
    let lease = commands.next().expect("genuine Ping lease");
    assert!(commands.next().is_none());
    assert!(matches!(lease.kind(), CommandKind::SendText { text } if text == "ping"));
    drop(result);
    lease
}

fn read(path: &Path) -> (Vec<RecordFrame>, PhysicalReport) {
    let mut reader = WalReader::open(path).unwrap();
    let mut records = Vec::new();
    for _ in 0..256 {
        match reader.next_record() {
            Ok(Some(frame)) => records.push(frame),
            Ok(None) | Err(_) => return (records, reader.report().clone()),
        }
    }
    panic!("bounded synthetic reader exceeded demo ceiling");
}

fn close_owner(driver: &mut Driver) -> domain::capture_session::CloseOwnerRef {
    driver.local_close().unwrap()
}

fn settle_close(driver: &mut Driver, owner: domain::capture_session::CloseOwnerRef) {
    let CloseLeaseReport::Leased(lease) = driver.owner.reclaim_close(&mut driver.turn, owner)
    else {
        panic!("same owner must reclaim its lawful Close");
    };
    dispatch(driver, [lease.into_command().unwrap()]);
}

fn finish(driver: &mut Driver) {
    // Finish any already admitted library restoration work after its Close receipt.
    let mut empty = false;
    for _ in 0..256 {
        let report = driver.drain().unwrap();
        eligible(report.session_disposition);
        match report.outcome.unwrap() {
            Some(result) => settle_result(driver, result),
            None => {
                empty = true;
                break;
            }
        }
    }
    assert!(empty, "bounded restoration drain");
    let owner = close_owner(driver);
    let ticket = driver.owner.begin_finalization(&mut driver.turn).unwrap();
    if driver
        .owner
        .outstanding_close_owners()
        .iter()
        .any(|view| view.owner == owner && view.state != CloseState::Settled)
    {
        assert!(matches!(
            driver.supervisor.quiesce(&mut driver.turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        settle_close(driver, owner);
    }
    let QuiescenceReport::Ready(mut proof) = driver.supervisor.quiesce(&mut driver.turn, &ticket)
    else {
        panic!("released healthy work and settled Close must be Ready");
    };
    let finalized = driver.owner.finalize(&mut driver.turn, &mut proof).unwrap();
    assert_eq!(finalized.input_quality(), InputQuality::Unknown);
}

#[test]
fn nominal_round_trip_preserves_bytes_stamps_dense_receive_order() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    for (bytes, time) in [(BOOK_ONE, 102), (BOOK_TWO, 103), (BOOK_THREE, 104)] {
        text(&mut driver, bytes, time);
        let result = drain(&mut driver);
        settle_result(&mut driver, result);
    }
    finish(&mut driver);
    let (records, report) = read(&path.0);
    assert_eq!(report.status, ArchiveStatus::Complete);
    assert_eq!(report.input_quality, Some(InputQuality::Unknown));
    for (index, frame) in records.iter().enumerate() {
        assert_eq!(frame.record_no.get(), u64::try_from(index).unwrap() + 1);
    }
    let raw: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::RawInput(raw) => Some(raw),
            _ => None,
        })
        .collect();
    assert_eq!(raw.len(), 4);
    for (raw, (bytes, time)) in raw.iter().zip([
        (ACK, 101),
        (BOOK_ONE, 102),
        (BOOK_TWO, 103),
        (BOOK_THREE, 104),
    ]) {
        assert_eq!(raw.bytes, bytes);
        let expected = stamp(time).unwrap();
        assert_eq!(raw.context.monotonic_ns.get(), expected.monotonic_ns);
        assert_eq!(raw.context.unix_ns.get(), expected.unix_ns);
    }
    assert!(!records.iter().any(|frame| matches!(
        &frame.value,
        Record::Control(control) if matches!(control.value, Control::Transport { value: Transport::Down, .. })
    )), "local stop does not invent a received Disconnected");
}

#[test]
fn timer_due_boundaries_duplicate_and_delayed_drain_keep_original_deadlines() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    assert!(admission(tick(&mut driver, PING - 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 0);
    assert!(admission(tick(&mut driver, PING)).is_empty());
    assert!(admission(tick(&mut driver, PING + 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 1);
    // Later observations cannot change the already admitted Timer stamp.
    text(&mut driver, BOOK_ONE, PING + 10);
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    assert!(admission(tick(&mut driver, TIMEOUT - 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 0);
    assert!(admission(tick(&mut driver, TIMEOUT)).is_empty());
    assert!(admission(tick(&mut driver, TIMEOUT + 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 1);
    let result = drain(&mut driver);
    assert_eq!(result.records.len(), 2);
    settle_result(&mut driver, result);
    finish(&mut driver);
    let (records, report) = read(&path.0);
    assert_eq!(report.status, ArchiveStatus::Complete);
    let timers: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::Control(control) => match control.value {
                Control::Timer {
                    timer_id,
                    deadline_ns,
                    ..
                } => Some((timer_id, deadline_ns, control.context.monotonic_ns.get())),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(timers, vec![(1, PING, PING), (2, TIMEOUT, TIMEOUT)]);
}

#[test]
fn pong_first_and_timeout_first_keep_admission_order_at_d_minus_one_d_d_plus_one() {
    for pong_time in [TIMEOUT - 1, TIMEOUT, TIMEOUT + 1] {
        for pong_first in [true, false] {
            let path = TempWal::new();
            let mut driver = prepared(&path.0, budget());
            let lease = ping(&mut driver);
            dispatch(&mut driver, [lease]);
            if pong_first {
                text(&mut driver, b"pong", pong_time);
                assert!(admission(tick(&mut driver, TIMEOUT.max(pong_time))).is_empty());
            } else {
                assert!(admission(tick(&mut driver, TIMEOUT)).is_empty());
                text(&mut driver, b"pong", pong_time);
            }
            for _ in 0..256 {
                if driver.supervisor.queued_items() == 0 {
                    break;
                }
                let result = drain(&mut driver);
                settle_result(&mut driver, result);
            }
            assert_eq!(driver.supervisor.queued_items(), 0);
            finish(&mut driver);
            let (records, report) = read(&path.0);
            assert_eq!(report.status, ArchiveStatus::Complete);
            let down_count = records.iter().filter(|frame| matches!(
                &frame.value, Record::Control(control)
                if matches!(control.value, Control::Transport { value: Transport::Down, .. })
            )).count();
            assert_eq!(down_count, usize::from(!pong_first));
            let pong_up = records
                .iter()
                .filter(|frame| {
                    matches!(
                        &frame.value, Record::Control(control)
                        if matches!(control.value, Control::Transport { value: Transport::Up, .. })
                    )
                })
                .count();
            assert_eq!(pong_up, 1 + usize::from(pong_first));
        }
    }
}

#[test]
fn late_tick_and_delayed_ping_effect_keep_timeout_at_original_recorded_stamp() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    let recorded = PING.checked_add(17).unwrap();
    let timeout = recorded.checked_add(15_000_000_000).unwrap();
    assert!(admission(tick(&mut driver, recorded)).is_empty());
    let mut result = drain(&mut driver);
    let lease = std::mem::take(&mut result.commands)
        .into_iter()
        .next()
        .unwrap();
    drop(result);
    assert!(admission(tick(&mut driver, timeout - 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 0);
    dispatch(&mut driver, [lease]);
    assert!(admission(tick(&mut driver, timeout)).is_empty());
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    finish(&mut driver);
    let (records, physical) = read(&path.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    let timers: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.value {
            Record::Control(control) => match control.value {
                Control::Timer { deadline_ns, .. } => {
                    Some((deadline_ns, control.context.monotonic_ns.get()))
                }
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(timers, vec![(PING, recorded), (timeout, timeout)]);
}

#[test]
fn held_ping_revocation_by_pong_closing_or_timeout_never_calls_transport() {
    for reason in 0..3 {
        let path = TempWal::new();
        let mut driver = prepared(&path.0, budget());
        let lease = ping(&mut driver);
        let ticket = if reason == 1 {
            Some(driver.owner.begin_finalization(&mut driver.turn).unwrap())
        } else {
            None
        };
        if reason == 0 {
            text(&mut driver, b"pong", PING + 1);
            let result = drain(&mut driver);
            settle_result(&mut driver, result);
        } else if reason == 2 {
            assert!(admission(tick(&mut driver, TIMEOUT)).is_empty());
            let result = drain(&mut driver);
            settle_result(&mut driver, result);
        }
        let before = driver.effects.len();
        assert!(matches!(
            driver.dispatch(lease, false).unwrap(),
            DispatchReport::Revoked(AuthorityError::CommandRevoked)
        ));
        assert_eq!(driver.effects.len(), before);
        if let Some(ticket) = ticket {
            let owner = close_owner(&mut driver);
            settle_close(&mut driver, owner);
            let QuiescenceReport::Ready(mut proof) =
                driver.supervisor.quiesce(&mut driver.turn, &ticket)
            else {
                panic!("revoked lease released");
            };
            driver.owner.finalize(&mut driver.turn, &mut proof).unwrap();
        } else {
            finish(&mut driver);
        }
        assert_eq!(read(&path.0).1.status, ArchiveStatus::Complete);
    }
}

#[test]
fn failed_ping_retains_one_callback_and_unknown_effect_without_retry() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    let lease = ping(&mut driver);
    let before = driver.effects.len();
    assert!(matches!(
        driver.dispatch(lease, true).unwrap(),
        DispatchReport::DispatchFailed {
            effect: AmbiguousEffect::Unknown,
            ..
        }
    ));
    assert_eq!(driver.effects.len(), before + 1);
    assert!(!driver.owner.session_status().failed);
    assert!(admission(tick(&mut driver, PING + 1)).is_empty());
    assert_eq!(driver.supervisor.queued_items(), 0);
    assert!(admission(tick(&mut driver, TIMEOUT)).is_empty());
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    finish(&mut driver);
    assert_eq!(read(&path.0).1.status, ArchiveStatus::Complete);
}

#[test]
fn failed_and_dropped_close_preserve_same_pending_owner_and_lawful_reclaim() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    let owner = close_owner(&mut driver);
    let CloseLeaseReport::Leased(lease) =
        driver.owner.reclaim_close(&mut driver.turn, owner.clone())
    else {
        panic!("lease");
    };
    assert!(matches!(
        driver.owner.reclaim_close(&mut driver.turn, owner.clone()),
        CloseLeaseReport::AlreadyLeased
    ));
    drop(lease);
    let view = driver.owner.outstanding_close_owners();
    assert_eq!(view.iter().next().unwrap().owner, owner);
    assert_eq!(view.iter().next().unwrap().state, CloseState::Pending);
    let CloseLeaseReport::Leased(lease) =
        driver.owner.reclaim_close(&mut driver.turn, owner.clone())
    else {
        panic!("reclaim after Drop");
    };
    let before = driver.effects.len();
    assert!(matches!(
        driver
            .dispatch(lease.into_command().unwrap(), true)
            .unwrap(),
        DispatchReport::DispatchFailed {
            effect: AmbiguousEffect::Unknown,
            ..
        }
    ));
    assert_eq!(driver.effects.len(), before + 1);
    let view = driver.owner.outstanding_close_owners();
    assert_eq!(view.iter().next().unwrap().owner, owner);
    assert_eq!(view.iter().next().unwrap().state, CloseState::Pending);
    settle_close(&mut driver, owner.clone());
    assert_eq!(driver.effects.len(), before + 2);
    assert!(matches!(
        driver.owner.reclaim_close(&mut driver.turn, owner),
        CloseLeaseReport::AlreadySettled
    ));
    // Two callback attempts after ambiguity are observable, not an exactly-once claim.
    finish(&mut driver);
    assert_eq!(read(&path.0).1.status, ArchiveStatus::Complete);
}

#[test]
fn third_close_attempt_is_a_bounded_refusal_preserving_same_unresolved_owner() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    let owner = close_owner(&mut driver);
    for _ in 0..2 {
        let CloseLeaseReport::Leased(lease) =
            driver.owner.reclaim_close(&mut driver.turn, owner.clone())
        else {
            panic!("pending original Close");
        };
        assert!(matches!(
            driver
                .dispatch(lease.into_command().unwrap(), true)
                .unwrap(),
            DispatchReport::DispatchFailed {
                effect: AmbiguousEffect::Unknown,
                ..
            }
        ));
    }
    let before = driver.effects.len();
    let CloseLeaseReport::Leased(lease) =
        driver.owner.reclaim_close(&mut driver.turn, owner.clone())
    else {
        panic!("third lease can be retained");
    };
    assert_eq!(
        driver
            .dispatch(lease.into_command().unwrap(), false)
            .expect_err("driver retry ceiling")
            .code,
        "close_attempt_bound"
    );
    assert_eq!(driver.effects.len(), before);
    let snapshot = driver.owner.outstanding_close_owners();
    assert_eq!(snapshot.iter().next().unwrap().owner, owner);
    assert_eq!(snapshot.iter().next().unwrap().state, CloseState::Pending);
    let report = driver.owner.close_diagnostic(&mut driver.turn);
    assert!(
        report.outcome.is_err(),
        "healthy archive cannot fabricate diagnostic completion for an unresolved Close"
    );
    assert_eq!(
        report.outstanding_close_owners.iter().next().unwrap().state,
        CloseState::Pending
    );
    assert_eq!(read(&path.0).1.status, ArchiveStatus::ValidPrefixIncomplete);
}

#[test]
fn healthy_shutdown_held_result_lease_and_close_prevent_ready_then_finalize_once() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    text(&mut driver, BOOK_ONE, 102);
    let held_result = drain(&mut driver);
    let owner = close_owner(&mut driver);
    let CloseLeaseReport::Leased(held_close) =
        driver.owner.reclaim_close(&mut driver.turn, owner.clone())
    else {
        panic!("close");
    };
    let ticket = driver.owner.begin_finalization(&mut driver.turn).unwrap();
    let QuiescenceReport::NotReady(summary) = driver.supervisor.quiesce(&mut driver.turn, &ticket)
    else {
        panic!("held owners block proof");
    };
    assert!(summary.results > 0);
    assert_eq!(
        summary.close_owners.iter().next().unwrap().state,
        CloseState::Leased
    );
    drop(held_close);
    settle_close(&mut driver, owner);
    assert!(matches!(
        driver.supervisor.quiesce(&mut driver.turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(held_result);
    let QuiescenceReport::Ready(mut proof) = driver.supervisor.quiesce(&mut driver.turn, &ticket)
    else {
        panic!("release");
    };
    let finalized = driver.owner.finalize(&mut driver.turn, &mut proof).unwrap();
    assert_eq!(finalized.input_quality(), InputQuality::Unknown);
    let bytes = fs::read(&path.0).unwrap();
    assert!(driver.owner.finalize(&mut driver.turn, &mut proof).is_err());
    assert_eq!(
        driver.owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    assert!(matches!(
        driver.sink.authority().mandatory_close(
            &mut driver.turn,
            binding().id,
            binding().tag.connection,
            None
        ),
        Err(AuthorityError::SessionClosed)
    ));
    let late = driver.supervisor.start_commands(&mut driver.turn);
    assert!(matches!(
        late.session_disposition,
        SessionDisposition::Closed(_)
    ));
    assert!(late.outcome.is_err());
    assert!(late.commands.is_empty());
    assert_eq!(fs::read(&path.0).unwrap(), bytes);
    let (records, physical) = read(&path.0);
    assert_eq!(physical.status, ArchiveStatus::Complete);
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|f| matches!(f.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
}

#[test]
fn raw_frame_byte_and_message_loss_coalesce_truthful_gap_without_failed_latch() {
    for cause in 0..3 {
        let path = TempWal::new();
        let limits = match cause {
            0 => RetentionBudget {
                raw_frame_limit: 1,
                ..budget()
            },
            1 => RetentionBudget {
                raw_byte_limit: BOOK_ONE.len(),
                max_message_bytes: BOOK_ONE.len(),
                ..budget()
            },
            _ => RetentionBudget {
                max_message_bytes: ACK.len(),
                ..budget()
            },
        };
        let mut driver = prepared(&path.0, limits);
        let b = binding();
        let loss_bytes: &[u8] = if cause == 2 { BOOK_ONE } else { BOOK_TWO };
        if cause != 2 {
            text(&mut driver, BOOK_ONE, 102);
        }
        for (index, time) in [103, 104].into_iter().enumerate() {
            let report = driver.supervisor.queue_text(
                &mut driver.turn,
                b.connection_id,
                b.tag.connection,
                stamp(time).unwrap(),
                loss_bytes,
            );
            eligible(report.session_disposition);
            assert_eq!(
                report.outcome,
                Ok(if index == 0 {
                    AdmissionOutcome::Admitted
                } else {
                    AdmissionOutcome::CoalescedLoss
                })
            );
            assert!(report.failure.is_none());
            assert!(report.close_owner.is_none());
            assert!(report.commands.is_empty());
        }
        assert!(!driver.owner.session_status().failed);
        assert!(
            driver
                .owner
                .outstanding_close_owners()
                .iter()
                .next()
                .is_none()
        );
        for _ in 0..256 {
            if driver.supervisor.queued_items() == 0 {
                break;
            }
            let result = drain(&mut driver);
            settle_result(&mut driver, result);
        }
        assert_eq!(driver.supervisor.queued_items(), 0);
        let snapshot = driver.supervisor.snapshot(b.id).unwrap();
        assert_eq!(
            snapshot.capture_attempt_frontier,
            if cause == 2 { 3 } else { 4 }
        );
        assert_eq!(
            snapshot.accounted_attempt_frontier,
            snapshot.capture_attempt_frontier
        );
        finish(&mut driver);
        let (records, physical) = read(&path.0);
        assert_eq!(physical.status, ArchiveStatus::Complete);
        let gaps: Vec<_> = records
            .iter()
            .filter_map(|f| match &f.value {
                Record::Gap(gap) => Some(gap),
                _ => None,
            })
            .collect();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].context.monotonic_ns.get(), 103);
        let domain::record::GapScope::ExplicitTargets(targets) = &gaps[0].scope else {
            panic!("explicit loss");
        };
        assert_eq!(targets[0].loss_count, Some(2));
        let range = targets[0].range.unwrap();
        assert_eq!(
            (range.0.get(), range.1.get()),
            if cause == 2 { (2, 3) } else { (3, 4) }
        );
    }
}

#[test]
fn received_disconnect_is_a_recorded_observation_and_transport_error_is_only_an_effect() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    let b = binding();
    let report = driver.supervisor.queue_disconnected(
        &mut driver.turn,
        b.connection_id,
        b.tag.connection,
        stamp(102).unwrap(),
    );
    assert!(admission(report).is_empty());
    let result = drain(&mut driver);
    settle_result(&mut driver, result);
    assert!(!driver.owner.session_status().failed);
    finish(&mut driver);
    let (records, _) = read(&path.0);
    assert!(records.iter().any(|frame| matches!(&frame.value,
        Record::Control(control) if control.context.monotonic_ns.get() == 102
        && matches!(control.value, Control::Transport { value: Transport::Down, .. }))));
}

#[test]
fn write_failure_in_closing_invalidates_proof_no_retry_no_seals_and_reports_undrained_owner() {
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    text(&mut driver, BOOK_ONE, 102);
    let ticket = driver.owner.begin_finalization(&mut driver.turn).unwrap();
    assert!(matches!(
        driver.supervisor.quiesce(&mut driver.turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    driver
        .owner
        .set_sink_fault(
            &mut driver.turn,
            Some(recording::SinkFault {
                at: RecordNo::new(7).unwrap(),
                kind: recording::SinkFaultKind::BeforeWrite(
                    domain::capture_session::PersistError::typed(
                        domain::capture_session::PersistErrorKind::Io,
                        "synthetic BeforeWrite",
                    ),
                ),
            }),
        )
        .unwrap();
    let report = driver.drain().unwrap();
    assert!(matches!(
        report.session_disposition,
        SessionDisposition::DiagnosticOnly { .. }
    ));
    assert!(matches!(
        report.outcome,
        Err(SupervisorError::Persistence(_))
    ));
    assert!(driver.owner.session_status().storage_stopped.is_some());
    assert!(matches!(
        driver.supervisor.quiesce(&mut driver.turn, &ticket),
        QuiescenceReport::FinalizationInvalidated(_)
    ));
    let before_calls = driver.owner.sink_persist_calls();
    let before = fs::read(&path.0).unwrap();
    let retry = driver.drain().unwrap();
    assert!(matches!(
        retry.session_disposition,
        SessionDisposition::DiagnosticOnly { .. }
    ));
    assert!(retry.outcome.is_err());
    assert_eq!(driver.owner.sink_persist_calls(), before_calls);
    assert_eq!(fs::read(&path.0).unwrap(), before);
    assert!(driver.owner.set_sink_fault(&mut driver.turn, None).is_err());
    let owner = close_owner(&mut driver);
    settle_close(&mut driver, owner);
    let report = driver.owner.close_diagnostic(&mut driver.turn);
    assert!(report.outcome.is_ok());
    assert!(report.physical_report.descriptor_closed);
    assert!(report.undrained_owners.work_total > 0);
    assert_eq!(report.input_completeness, InputQuality::Unknown);
    let (records, physical) = read(&path.0);
    assert_eq!(records.len(), 6);
    assert_eq!(physical.status, ArchiveStatus::ValidPrefixIncomplete);
    assert_eq!(physical.input_quality, None);
    assert!(
        !records
            .iter()
            .any(|f| matches!(f.value, Record::SegmentSeal(_) | Record::ArchiveSeal(_)))
    );
}

#[test]
fn scripts_have_repeatable_normalized_outcomes_and_unverified_quality() {
    for scenario in Scenario::ALL {
        let first = TempWal::new();
        let second = TempWal::new();
        let a = capture_driver::run_scenario(&first.0, scenario).unwrap();
        let b = capture_driver::run_scenario(&second.0, scenario).unwrap();
        assert_eq!(a.json(), b.json());
        assert_eq!(fs::read(&first.0).unwrap(), fs::read(&second.0).unwrap());
        let json = a.json();
        for field in [
            "\"synthetic\":true",
            "\"canonical_status\":\"NotEvaluated\"",
            "\"canonical_applicability\":\"BLOCKED_UNVERIFIED\"",
            "\"usable_data\":false",
        ] {
            assert!(json.contains(field), "required quality field {field}");
        }
        assert!(!json.contains(first.0.to_str().unwrap()));
        if scenario.exit_code() == 0 {
            assert_eq!(a.physical.status, ArchiveStatus::Complete);
            assert_eq!(a.physical.input_quality, Some(InputQuality::Unknown));
        }
    }
}

#[test]
fn existing_path_and_unknown_scenario_do_not_overwrite() {
    let path = TempWal::new();
    let original = b"existing bytes must stay unchanged";
    fs::write(&path.0, original).unwrap();
    assert!(capture_driver::run_scenario(&path.0, Scenario::Nominal).is_err());
    assert_eq!(fs::read(&path.0).unwrap(), original);
    assert!(Scenario::parse("unknown").is_err());
    assert!(stamp(u64::MAX).is_err());
}

#[test]
fn scripted_observation_payload_and_total_byte_bounds_are_checked_before_capture() {
    use capture_driver::{ScriptInput, validate_inputs};
    let s = stamp(102).unwrap();
    let observations = vec![ScriptInput::Tick(s); 65];
    assert_eq!(
        validate_inputs(&observations).unwrap_err().code,
        "observation_bound"
    );
    let payload = vec![0; 4097];
    assert_eq!(
        validate_inputs(&[ScriptInput::Text(s, &payload)])
            .unwrap_err()
            .code,
        "payload_bound"
    );
    let payload = vec![0; 4096];
    let bytes = vec![ScriptInput::Text(s, &payload); 17];
    assert_eq!(
        validate_inputs(&bytes).unwrap_err().code,
        "script_bytes_bound"
    );
    assert!(validate_inputs(&bytes[..16]).is_ok());
    // Incremental port preserves counters and bytes on the first rejected input.
    let path = TempWal::new();
    let mut driver = prepared(&path.0, budget());
    for _ in 0..64 {
        let report = driver.tick(s).unwrap();
        assert!(admission(report).is_empty());
    }
    let before = fs::read(&path.0).unwrap();
    assert_eq!(
        driver.tick(s).err().expect("bounded refusal").code,
        "observation_bound"
    );
    assert_eq!(driver.observations, 64);
    assert_eq!(fs::read(&path.0).unwrap(), before);
    finish(&mut driver);
}

#[test]
fn reader_corrupt_and_torn_copies_never_complete_or_skip_and_original_is_unchanged() {
    let original_path = TempWal::new();
    capture_driver::run_scenario(&original_path.0, Scenario::Nominal).unwrap();
    let original = fs::read(&original_path.0).unwrap();
    let mut offsets = Vec::new();
    let mut cursor = 0usize;
    while cursor < original.len() {
        let frame =
            recording::scan_frame(&original[cursor..], u64::try_from(cursor).unwrap()).unwrap();
        offsets.push((cursor, frame.length()));
        cursor = cursor.checked_add(frame.length()).unwrap();
    }
    let (middle, length) = offsets[6];
    for damage in 0..5 {
        let copy = TempWal::new();
        let mut bytes = original.clone();
        let expected_prefix = match damage {
            0 => {
                bytes[middle + length - 1] ^= 1;
                middle
            }
            1 => {
                bytes.truncate(middle + recording::HEADER_LEN - 1);
                middle
            }
            2 => {
                bytes.truncate(middle + recording::HEADER_LEN + 1);
                middle
            }
            3 => {
                let last = offsets.last().unwrap().0;
                bytes.truncate(last);
                last
            }
            _ => {
                bytes.extend_from_slice(b"trailing");
                original.len()
            }
        };
        fs::write(&copy.0, bytes).unwrap();
        let (records, physical) = read(&copy.0);
        assert_ne!(physical.status, ArchiveStatus::Complete);
        assert_eq!(
            physical.physical_good_offset,
            u64::try_from(expected_prefix).unwrap()
        );
        match damage {
            0 => {
                assert_eq!(physical.status, ArchiveStatus::Corrupt);
                assert!(
                    matches!(physical.failure.unwrap().kind, FailureKind::Codec(error) if error.kind == CodecErrorKind::ChecksumMismatch)
                );
                assert_eq!(records.last().unwrap().record_no.get(), 6);
            }
            1 | 2 => {
                assert_eq!(physical.status, ArchiveStatus::TruncatedTail);
                assert!(
                    matches!(physical.failure.unwrap().kind, FailureKind::Codec(error) if error.kind == CodecErrorKind::TruncatedTail)
                );
                assert_eq!(records.last().unwrap().record_no.get(), 6);
            }
            3 => {
                assert_eq!(
                    physical.status,
                    ArchiveStatus::SegmentSealedArchiveIncomplete
                );
                assert_eq!(physical.input_quality, None);
                assert!(physical.failure.is_none());
            }
            _ => {
                assert_eq!(physical.status, ArchiveStatus::Invalid);
                assert!(matches!(
                    physical.failure.unwrap().kind,
                    FailureKind::Validation(recording::ValidationError::TrailingData)
                ));
            }
        }
        assert_eq!(fs::read(&original_path.0).unwrap(), original);
    }
}
