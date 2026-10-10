//! Public receipt-stage conformance for the synthetic F-2 filesystem owner.
//! The supervisor drains Timeout Timer and Down atomically. This separate test
//! uses its existing public handle port to inspect the intermediate Close gate;
//! the library still selects every Timer identity, kind, deadline and plan.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use domain::capture_session::{
    AdmittedTimer, AuthorityError, CloseLeaseReport, CloseState, CloseStorage, CommandKind,
    DispatchReport, HeartbeatPolicy, ObservationClass, ObservationIdentity, QuiescenceReport,
    ReceiveStamp, ScopeBinding, SessionLifecycle, SupervisorSessionHandle, TimerAdmission,
    TimerKind, TimerProgressView, WorkKind,
};
use domain::event::InputContext;
use domain::identity::{LocalUnixNs, MonotonicNs};
use domain::policy::RecordingGate;
use domain::record::{
    Control, ControlRecord, InputQuality, Record, RecordFrame, Transport, WireContext,
};
use recording::{
    ArchiveStatus, BoundedCaptureProfile, CaptureSessionOwner, PhysicalReport, WalReader,
};

use crate::capture_driver::{binding, bootstrap, budget, stamp, start_frame};

static NEXT_STAGE_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct StageWal {
    directory: PathBuf,
    path: PathBuf,
}

impl StageWal {
    fn new() -> Self {
        let serial = NEXT_STAGE_DIRECTORY
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("finite test directory counter");
        let directory = std::env::temp_dir().join(format!(
            "proscalping-f2-timer-stage-{}-{serial}",
            std::process::id()
        ));
        // A collision fails; neither this test nor create_new overwrites a file.
        fs::create_dir(&directory).expect("fresh stage-test directory");
        let path = directory.join("capture.wal");
        Self { directory, path }
    }
}

impl Drop for StageWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir(&self.directory);
    }
}

fn receive(monotonic_ns: u64) -> ReceiveStamp {
    let scripted = stamp(monotonic_ns).expect("checked fixed synthetic stamp");
    ReceiveStamp {
        unix_ns: scripted.unix_ns,
        monotonic_ns: scripted.monotonic_ns,
    }
}

fn context(handle: &SupervisorSessionHandle, original: ObservationIdentity) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(original.stamp.unix_ns),
        monotonic_ns: MonotonicNs::new(original.stamp.monotonic_ns),
        context: InputContext::Active(handle.prefix().context),
    }
}

fn transport(
    handle: &SupervisorSessionHandle,
    original: ObservationIdentity,
    value: Transport,
) -> RecordFrame {
    let prefix = handle.prefix();
    RecordFrame {
        record_no: prefix.next_record,
        segment_no: prefix.segment,
        value: Record::Control(ControlRecord {
            context: context(handle, original),
            value: Control::Transport {
                connection: binding().connection_id,
                epoch: original.epoch,
                value,
            },
        }),
    }
}

fn original_timer(handle: &SupervisorSessionHandle, timer: &AdmittedTimer) -> RecordFrame {
    let original = timer.identity();
    let ObservationClass::Timer {
        timer_id,
        deadline_ns,
    } = original.class
    else {
        panic!("authority-returned original Timer identity required")
    };
    let prefix = handle.prefix();
    RecordFrame {
        record_no: prefix.next_record,
        segment_no: prefix.segment,
        value: Record::Control(ControlRecord {
            context: context(handle, original),
            value: Control::Timer {
                stream: original.stream,
                timer_id,
                deadline_ns,
            },
        }),
    }
}

fn read_stage_wal(path: &Path) -> (Vec<RecordFrame>, PhysicalReport) {
    let mut reader = WalReader::open(path).expect("existing reader opens genuine owner WAL");
    let mut records = Vec::new();
    for _ in 0..16 {
        match reader
            .next_record()
            .expect("physical owner prefix validates")
        {
            Some(record) => records.push(record),
            None => return (records, reader.report().clone()),
        }
    }
    panic!("fixed stage WAL exceeded its sixteen-record test bound")
}

#[test]
fn durable_original_timeout_timer_only_preserves_close_until_original_down_and_finalizes_once() {
    let wal = StageWal::new();
    let definitions = bootstrap();
    let stream = binding();
    let (mut owner, mut turn) = CaptureSessionOwner::create_new(
        &wal.path,
        &start_frame(),
        BoundedCaptureProfile::new(&definitions),
    )
    .expect("fresh actual filesystem owner with synthetic Durable profile");
    let (handle, mut sink) = owner
        .register_supervisor(
            &mut turn,
            &[ScopeBinding {
                stream: stream.id,
                connection: stream.connection_id,
                epoch: stream.tag.connection,
            }],
            budget(),
            HeartbeatPolicy::SupervisorV2,
        )
        .expect("one accepted owner-minted public handle and bound sink");
    assert_eq!(handle.prefix().recording_gate, RecordingGate::Durable);

    let connected = ObservationIdentity {
        stream: stream.id,
        epoch: stream.tag.connection,
        stamp: receive(100),
        class: ObservationClass::Connected,
        tag: None,
        attempts: None,
        loss_count: None,
    };
    let received = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .expect("counted received work");
    handle
        .admit_observation(&mut turn, &received, connected)
        .expect("original received identity admitted once");
    received
        .set_kind(&mut turn, WorkKind::InFlightObservation)
        .expect("transfer original observation ownership");
    let up = transport(&handle, connected, Transport::Up);
    assert_eq!(up.record_no.get(), 5);
    sink.persist_owned(&mut turn, &up, RecordingGate::Durable, &received)
        .expect("exact original Up receipt installs library schedule");
    handle
        .complete_observation(&mut turn, &sink, &received, None)
        .expect("authenticated received completion");
    drop(received);

    let policy = HeartbeatPolicy::SupervisorV2;
    let ping_at = connected
        .stamp
        .monotonic_ns
        .checked_add(policy.ping_interval_ns())
        .expect("checked fixed Ping observation stamp");
    let TimerAdmission::Admitted(ping) = handle
        .admit_due_timer(&mut turn, stream.id, receive(ping_at))
        .expect("library derives original due Timer")
    else {
        panic!("one original Ping is due")
    };
    assert_eq!(ping.kind(), TimerKind::Ping);
    ping.owner()
        .set_kind(&mut turn, WorkKind::InFlightObservation)
        .expect("transfer original Ping");
    let ping_record = original_timer(&handle, &ping);
    assert_eq!(ping_record.record_no.get(), 6);
    sink.persist_owned(
        &mut turn,
        &ping_record,
        RecordingGate::Durable,
        ping.owner(),
    )
    .expect("exact Ping Timer receipt creates its affine command");
    let ping_command = handle
        .take_timer_ping(&mut turn, ping.owner())
        .expect("extract sole original Ping entitlement");
    handle
        .complete_observation(&mut turn, &sink, ping.owner(), None)
        .expect("settle original Ping receipt");
    let mut callbacks = 0;
    assert!(matches!(
        owner.dispatch(&mut turn, ping_command, |view| {
            callbacks += 1;
            assert!(matches!(view.kind, CommandKind::SendText { text } if text == "ping"));
            Ok::<(), ()>(())
        }),
        DispatchReport::Dispatched
    ));
    drop(ping);

    let timeout_at = ping_at
        .checked_add(policy.pong_timeout_ns())
        .expect("checked original-stamp Timeout deadline");
    let TimerAdmission::Admitted(timeout) = handle
        .admit_due_timer(&mut turn, stream.id, receive(timeout_at))
        .expect("authority admits the original Timeout")
    else {
        panic!("one original Timeout is due")
    };
    assert_eq!(timeout.kind(), TimerKind::Timeout);
    timeout
        .owner()
        .set_kind(&mut turn, WorkKind::InFlightObservation)
        .expect("transfer original Timeout");
    let timer_record = original_timer(&handle, &timeout);
    assert_eq!(timer_record.record_no.get(), 7);
    sink.persist_owned(
        &mut turn,
        &timer_record,
        RecordingGate::Durable,
        timeout.owner(),
    )
    .expect("only the exact original Timer stage is confirmed");
    let TimerProgressView::TimerThenDown {
        timer_confirmed: true,
        down_confirmed: false,
        close,
    } = handle.timer_progress(&mut turn, timeout.owner()).unwrap()
    else {
        panic!("authority selected Timer then Down on the original work")
    };
    assert_eq!(close.state, CloseState::Pending);
    assert!(!close.ready);
    assert_eq!(
        close.owner.storage(),
        CloseStorage::WorkOwner(timeout.owner().id())
    );
    let original_close = close.owner;

    let before_prefix = handle.prefix();
    let before_ledger = handle.authority().ownership_report();
    let before_status = owner.session_status();
    let before_closes = owner.outstanding_close_owners();
    let before_watermarks = owner.watermarks();
    let before_calls = owner.sink_persist_calls();
    let before_callbacks = callbacks;
    let before_bytes = fs::read(&wal.path).unwrap();
    let before_records = read_stage_wal(&wal.path);
    assert_eq!(before_records.0.len(), 7);
    assert_eq!(
        before_records.1.status,
        ArchiveStatus::ValidPrefixIncomplete
    );
    assert_eq!(before_records.1.input_quality, None);
    assert!(matches!(
        owner.reclaim_close(&mut turn, original_close.clone()),
        CloseLeaseReport::Rejected(AuthorityError::CloseNotReady)
    ));
    assert_eq!(
        handle.complete_observation(&mut turn, &sink, timeout.owner(), None),
        Err(AuthorityError::NotQuiescent)
    );
    assert_eq!(handle.prefix(), before_prefix);
    assert_eq!(handle.authority().ownership_report(), before_ledger);
    assert_eq!(owner.session_status(), before_status);
    assert_eq!(owner.outstanding_close_owners(), before_closes);
    assert_eq!(owner.watermarks(), before_watermarks);
    assert_eq!(owner.sink_persist_calls(), before_calls);
    assert_eq!(callbacks, before_callbacks);
    assert_eq!(fs::read(&wal.path).unwrap(), before_bytes);
    assert_eq!(read_stage_wal(&wal.path), before_records);

    let down = transport(&handle, timeout.identity(), Transport::Down);
    assert_eq!(down.record_no.get(), 8);
    sink.persist_owned(&mut turn, &down, RecordingGate::Durable, timeout.owner())
        .expect("same original Timeout Down, stamp and WorkOwner");
    let TimerProgressView::TimerThenDown {
        timer_confirmed: true,
        down_confirmed: true,
        close: ready_close,
    } = handle.timer_progress(&mut turn, timeout.owner()).unwrap()
    else {
        panic!("both exact original stages must be confirmed")
    };
    assert_eq!(ready_close.owner, original_close);
    assert!(ready_close.ready);
    handle
        .complete_observation(&mut turn, &sink, timeout.owner(), None)
        .expect("complete after the original Down and retained same Close");

    let ticket = owner.begin_finalization(&mut turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    let CloseLeaseReport::Leased(lease) = owner.reclaim_close(&mut turn, original_close.clone())
    else {
        panic!("same original Close is now reclaimable")
    };
    assert_eq!(lease.owner(), &original_close);
    assert!(matches!(
        owner.dispatch(&mut turn, lease.into_command().unwrap(), |view| {
            callbacks += 1;
            assert_eq!(view.connection, stream.connection_id);
            assert_eq!(view.epoch, stream.tag.connection);
            assert_eq!(view.kind, &CommandKind::Close);
            Ok::<(), ()>(())
        }),
        DispatchReport::Dispatched
    ));
    assert_eq!(callbacks, 2);
    assert!(matches!(
        owner.reclaim_close(&mut turn, original_close),
        CloseLeaseReport::AlreadySettled
    ));
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::NotReady(_)
    ));
    drop(timeout);
    let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
        panic!("same borrowed ticket becomes ready after real settlement and release")
    };
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    let finalized = owner.finalize(&mut turn, &mut proof).unwrap();
    assert_eq!(finalized.input_quality(), InputQuality::Unknown);
    assert_eq!(
        owner.session_status().lifecycle,
        SessionLifecycle::Finalized
    );
    let completed_bytes = fs::read(&wal.path).unwrap();
    let (records, report) = read_stage_wal(&wal.path);
    assert_eq!(report.status, ArchiveStatus::Complete);
    assert_eq!(report.input_quality, Some(InputQuality::Unknown));
    assert_eq!(records.len(), 10);
    assert_eq!(&records[4..8], &[up, ping_record, timer_record, down]);
    for (index, record) in records.iter().enumerate() {
        let expected = u64::try_from(index).unwrap().checked_add(1).unwrap();
        assert_eq!(record.record_no.get(), expected);
    }
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.value, Record::SegmentSeal(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.value, Record::ArchiveSeal(_)))
            .count(),
        1
    );
    assert!(owner.finalize(&mut turn, &mut proof).is_err());
    assert_eq!(
        handle.mandatory_close(&mut turn, stream.id, stream.tag.connection, None),
        Err(AuthorityError::SessionClosed)
    );
    assert_eq!(callbacks, 2);
    assert_eq!(fs::read(&wal.path).unwrap(), completed_bytes);
    assert_eq!(read_stage_wal(&wal.path).0, records);
}
