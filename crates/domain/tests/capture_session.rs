use domain::capture_session::*;
use domain::event::ActiveContext;
use domain::identity::*;
use domain::policy::RecordingGate;

fn session(
    n: usize,
    cap: usize,
) -> (
    CaptureSessionAuthority,
    SessionTurn,
    SupervisorSessionHandle,
) {
    let (authority, mut turn) = CaptureSessionAuthority::new(SessionBinding {
        archive: ArchiveId::new([1; 16]).unwrap(),
        session: CaptureSessionId::new([2; 16]).unwrap(),
        clock: ClockId::new(1).unwrap(),
    });
    let scopes: Vec<_> = (1..=n)
        .map(|id| ScopeBinding {
            stream: StreamId::new(id as u32).unwrap(),
            connection: ConnectionId::new(id as u32).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
        })
        .collect();
    let handle = authority
        .register_supervisor(
            &mut turn,
            &scopes,
            RetentionBudget {
                item_cap: cap,
                raw_frame_limit: 1,
                raw_byte_limit: 1024,
                max_message_bytes: 1024,
            },
            PrefixBinding {
                context: ActiveContext {
                    config: ConfigVersion::new(1).unwrap(),
                    normalizer: NormalizerVersion::new(1).unwrap(),
                },
                recording_gate: RecordingGate::Durable,
                segment: SegmentNo::new(0),
                next_record: RecordNo::new(10).unwrap(),
            },
        )
        .unwrap();
    (authority, turn, handle)
}

fn failure(stream: u32) -> TerminalFailure {
    TerminalFailure {
        stream: StreamId::new(stream).unwrap(),
        connection: ConnectionId::new(stream).unwrap(),
        observed_tag: EpochTag {
            spec: SpecVersion::new(1).unwrap(),
            connection: ConnectionEpoch::new(1).unwrap(),
            subscription: SubscriptionEpoch::new(1).unwrap(),
            book: Some(BookEpoch::new(1).unwrap()),
        },
        current_epoch: ConnectionEpoch::new(1).unwrap(),
        context: ActiveContext {
            config: ConfigVersion::new(1).unwrap(),
            normalizer: NormalizerVersion::new(1).unwrap(),
        },
        stamp: ReceiveStamp {
            unix_ns: 123,
            monotonic_ns: 456,
        },
        input_class: InputClass::Raw,
        attempt: AttemptIdentity::Candidate(CaptureAttemptNo::new(6).unwrap()),
        cause: FailureCause::QueueOverflow,
    }
}

fn close(
    authority: &CaptureSessionAuthority,
    turn: &mut SessionTurn,
    work: Option<&WorkOwner>,
) -> CloseOwnerRef {
    authority
        .mandatory_close(
            turn,
            StreamId::new(1).unwrap(),
            ConnectionEpoch::new(1).unwrap(),
            work,
        )
        .unwrap()
}

fn assert_same_ledger(actual: OwnershipReport, expected: OwnershipReport) {
    assert_eq!(actual.item_cap, expected.item_cap);
    assert_eq!(actual.reserved_scopes, expected.reserved_scopes);
    assert_eq!(actual.reserved_archive, expected.reserved_archive);
    assert_eq!(actual.work_limit, expected.work_limit);
    assert_eq!(actual.work_used, expected.work_used);
    assert_eq!(actual.pre_cut, expected.pre_cut);
    assert_eq!(actual.post_cut, expected.post_cut);
    assert!(actual.metadata_backing_bytes <= actual.metadata_ceiling_bytes);
}

fn lease(
    authority: &CaptureSessionAuthority,
    turn: &mut SessionTurn,
    owner: &CloseOwnerRef,
) -> CloseLease {
    match authority.reclaim_close(turn, owner.clone()) {
        CloseLeaseReport::Leased(lease) => lease,
        other => panic!("expected lease: {other:?}"),
    }
}

#[test]
fn legal_caps_reserve_all_terminal_owners_and_freeze_admission_cut() {
    for (n, cap, limit) in [(1, 5, 3), (2, 9, 6)] {
        let (authority, mut turn, handle) = session(n, cap);
        let mut owners = Vec::new();
        for _ in 0..limit {
            owners.push(
                handle
                    .reserve_work(&mut turn, WorkKind::QueuedObservation)
                    .unwrap(),
            );
        }
        assert_eq!(
            handle
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap_err(),
            AuthorityError::WorkExhausted
        );
        let terminal = handle.terminate(&mut turn, failure(1)).unwrap();
        assert!(terminal.first);
        assert!(
            owners
                .iter()
                .all(|owner| owner.cut_side() == CutSide::PreCut)
        );
        let before = authority.ownership_report();
        assert_eq!(
            before.work_used + before.reserved_scopes + before.reserved_archive,
            cap
        );
        assert!(before.metadata_backing_bytes <= before.metadata_ceiling_bytes);
        drop(owners.remove(0));
        let post = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        assert_eq!(post.cut_side(), CutSide::PostCut);
        assert!(
            owners
                .iter()
                .all(|owner| owner.cut_side() == CutSide::PreCut)
        );
        authority
            .storage_stopped(
                &mut turn,
                PersistError::typed(PersistErrorKind::Io, "injected failure"),
            )
            .unwrap();
        assert!(
            owners
                .iter()
                .all(|owner| owner.cut_side() == CutSide::PreCut)
        );
        assert_eq!(post.cut_side(), CutSide::PostCut);
        assert_eq!(authority.status().cut_sequence, Some(limit as u64));
    }
}

#[test]
fn terminal_identity_is_immutable_repeats_create_no_close_lease_or_work() {
    let (authority, mut turn, handle) = session(2, 9);
    let original = failure(1);
    let first = handle.terminate(&mut turn, original).unwrap();
    drop(first.close);
    let before = authority.ownership_report();
    for _ in 0..40 {
        let mut later = original;
        later.stamp.monotonic_ns += 1;
        later.attempt = AttemptIdentity::Candidate(CaptureAttemptNo::new(7).unwrap());
        let report = handle.terminate(&mut turn, later).unwrap();
        assert!(!report.first);
        assert!(report.close.is_none());
        assert_eq!(report.close_owner, first.close_owner);
        assert_eq!(authority.terminal_failure(original.stream), Some(original));
        assert_same_ledger(authority.ownership_report(), before);
    }
    let neighbor = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    assert_eq!(neighbor.cut_side(), CutSide::PostCut);
    assert!(matches!(
        authority.disposition(),
        SessionDisposition::DiagnosticOnly { .. }
    ));
}

#[test]
fn mandatory_close_drop_error_double_reclaim_and_success_preserve_owner_identity() {
    for transferred in [false, true] {
        let (authority, mut turn, handle) = session(1, 5);
        let work = transferred.then(|| {
            handle
                .reserve_work(&mut turn, WorkKind::PendingPlan)
                .unwrap()
        });
        let owner = close(&authority, &mut turn, work.as_ref());
        let before = authority.ownership_report();
        let first = lease(&authority, &mut turn, &owner);
        assert!(matches!(
            authority.reclaim_close(&mut turn, owner.clone()),
            CloseLeaseReport::AlreadyLeased
        ));
        assert_same_ledger(authority.ownership_report(), before);
        drop(first);
        assert_eq!(
            authority
                .outstanding_close_owners()
                .iter()
                .next()
                .unwrap()
                .state,
            CloseState::Pending
        );
        let second = lease(&authority, &mut turn, &owner);
        let result = authority.dispatch(&mut turn, second.into_command(), |_| {
            Err("possible physical effect")
        });
        assert!(matches!(
            result,
            DispatchReport::DispatchFailed {
                effect: AmbiguousEffect::Unknown,
                ..
            }
        ));
        assert_same_ledger(authority.ownership_report(), before);
        let third = lease(&authority, &mut turn, &owner);
        assert!(matches!(
            authority.dispatch(&mut turn, third.into_command(), |_| Ok::<_, ()>(())),
            DispatchReport::Dispatched
        ));
        assert!(matches!(
            authority.reclaim_close(&mut turn, owner),
            CloseLeaseReport::AlreadySettled
        ));
        assert_eq!(authority.outstanding_close_owners().iter().count(), 0);
        // Settlement releases only Close's share. The transferred plan remains.
        assert_eq!(
            authority.ownership_report().work_used,
            usize::from(transferred)
        );
        drop(work);
        assert_eq!(authority.ownership_report().work_used, 0);
    }
}

#[test]
fn foreign_reclaim_and_dispatch_leave_rightful_lease_owned() {
    let (authority, mut turn, _) = session(1, 5);
    let (foreign, mut foreign_turn, _) = session(1, 5);
    let owner = close(&authority, &mut turn, None);
    let before = authority.ownership_report();
    assert!(matches!(
        foreign.reclaim_close(&mut foreign_turn, owner.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        authority.reclaim_close(&mut foreign_turn, owner.clone()),
        CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    let command = lease(&authority, &mut turn, &owner).into_command();
    let returned = match foreign.dispatch(&mut foreign_turn, command, |_| -> Result<(), ()> {
        panic!("foreign effect")
    }) {
        DispatchReport::Denied {
            reason: AuthorityError::AuthorityMismatch,
            command,
        } => command,
        other => panic!("wrong foreign result: {other:?}"),
    };
    assert_eq!(returned.close_owner(), Some(&owner));
    assert_same_ledger(authority.ownership_report(), before);
    assert!(matches!(
        authority.reclaim_close(&mut turn, owner.clone()),
        CloseLeaseReport::AlreadyLeased
    ));
    drop(returned);
    assert!(matches!(
        authority.reclaim_close(&mut turn, owner),
        CloseLeaseReport::Leased(_)
    ));
}

#[test]
fn mandatory_close_survives_storage_stop_and_diagnostic_descriptor_closure() {
    let (authority, mut turn, handle) = session(1, 5);
    let work = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .unwrap();
    let owner = close(&authority, &mut turn, Some(&work));
    let terminal = handle.terminate(&mut turn, failure(1)).unwrap();
    assert_eq!(terminal.close_owner, owner);
    assert_eq!(owner.storage(), CloseStorage::WorkOwner(work.id()));
    drop(terminal.close);
    authority
        .storage_stopped(
            &mut turn,
            PersistError::typed(PersistErrorKind::Io, "stopped"),
        )
        .unwrap();
    authority.begin_diagnostic_close(&mut turn).unwrap();
    authority.diagnostic_closed(&mut turn).unwrap();
    let before = authority.ownership_report();
    let command = lease(&authority, &mut turn, &owner).into_command();
    drop(command);
    assert_same_ledger(authority.ownership_report(), before);
    let command = lease(&authority, &mut turn, &owner).into_command();
    assert!(matches!(
        authority.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
        DispatchReport::Dispatched
    ));
    assert_eq!(
        authority.status().lifecycle,
        SessionLifecycle::DiagnosticClosed
    );
    assert_eq!(authority.ownership_report().work_used, 1);
}

#[test]
fn borrowed_quiescence_not_ready_is_repeatable_and_only_one_proof_is_issued() {
    let (authority, mut turn, handle) = session(1, 5);
    let work = handle
        .reserve_work(&mut turn, WorkKind::QueuedObservation)
        .unwrap();
    let owner = close(&authority, &mut turn, None);
    let ticket = authority.begin_finalization(&mut turn).unwrap();
    assert_eq!(
        authority.begin_finalization(&mut turn).unwrap_err(),
        AuthorityError::AlreadyClosing
    );
    let before = authority.ownership_report();
    for _ in 0..10 {
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::NotReady(_)
        ));
        assert_same_ledger(authority.ownership_report(), before);
        assert_eq!(authority.status().lifecycle, SessionLifecycle::Closing);
    }
    assert_eq!(
        handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap_err(),
        AuthorityError::SessionClosing
    );
    drop(work);
    let command = lease(&authority, &mut turn, &owner).into_command();
    authority.dispatch(&mut turn, command, |_| Ok::<_, ()>(()));
    let proof = match handle.quiesce(&mut turn, &ticket) {
        QuiescenceReport::Ready(proof) => proof,
        other => panic!("{other:?}"),
    };
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::TicketConsumed
    ));
    authority.consume_proof(&mut turn, proof).unwrap();
    authority.ensure_finalization_authorized().unwrap();
    authority.finalization_finished(&mut turn).unwrap();
    assert_eq!(authority.status().lifecycle, SessionLifecycle::Finalized);
}

#[test]
fn foreign_ticket_and_turn_cannot_consume_the_rightful_ticket() {
    let (authority, mut turn, handle) = session(1, 5);
    let (foreign, mut foreign_turn, foreign_handle) = session(1, 5);
    let ticket = authority.begin_finalization(&mut turn).unwrap();
    let foreign_ticket = foreign.begin_finalization(&mut foreign_turn).unwrap();
    assert!(matches!(
        handle.quiesce(&mut turn, &foreign_ticket),
        QuiescenceReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        handle.quiesce(&mut foreign_turn, &ticket),
        QuiescenceReport::Rejected(AuthorityError::AuthorityMismatch)
    ));
    assert!(matches!(
        foreign_handle.quiesce(&mut foreign_turn, &foreign_ticket),
        QuiescenceReport::Ready(_)
    ));
    assert!(matches!(
        handle.quiesce(&mut turn, &ticket),
        QuiescenceReport::Ready(_)
    ));
}

#[test]
fn failure_during_closing_invalidates_active_ticket_or_already_issued_proof() {
    for after_ready in [false, true] {
        let (authority, mut turn, handle) = session(1, 5);
        let ticket = authority.begin_finalization(&mut turn).unwrap();
        let proof = if after_ready {
            match handle.quiesce(&mut turn, &ticket) {
                QuiescenceReport::Ready(proof) => Some(proof),
                other => panic!("{other:?}"),
            }
        } else {
            None
        };
        authority
            .storage_stopped(
                &mut turn,
                PersistError::typed(PersistErrorKind::Io, "failure during Closing"),
            )
            .unwrap();
        assert_eq!(
            authority.status().lifecycle,
            SessionLifecycle::DiagnosticClosing
        );
        for _ in 0..3 {
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
            ));
        }
        if let Some(proof) = proof {
            assert_eq!(
                authority.consume_proof(&mut turn, proof).unwrap_err(),
                AuthorityError::ArchiveFailed
            );
        }
        assert_eq!(
            authority.ensure_finalization_authorized().unwrap_err(),
            AuthorityError::ArchiveFailed
        );
        assert_eq!(
            authority.ensure_admission_open(&turn).unwrap_err(),
            AuthorityError::StorageStopped
        );
    }
}

#[test]
fn work_sharing_is_bounded_and_holds_exactly_one_logical_work_unit() {
    let (authority, mut turn, handle) = session(1, 5);
    let owner = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .unwrap();
    let shares = [
        owner.share().unwrap(),
        owner.share().unwrap(),
        owner.share().unwrap(),
    ];
    assert_eq!(
        owner.share().unwrap_err(),
        AuthorityError::WorkShareExhausted
    );
    assert_eq!(authority.ownership_report().work_used, 1);
    assert_eq!(authority.ownership_report().work_references, 4);
    drop(owner);
    assert_eq!(authority.ownership_report().work_used, 1);
    drop(shares);
    assert_eq!(authority.ownership_report().work_used, 0);
}

#[test]
fn one_work_owner_cannot_supply_three_scope_closes_at_cap_thirteen() {
    let (authority, mut turn, handle) = session(3, 13);
    let work = handle
        .reserve_work(&mut turn, WorkKind::PendingPlan)
        .unwrap();
    let epoch = ConnectionEpoch::new(1).unwrap();
    let rightful = authority
        .mandatory_close(&mut turn, StreamId::new(1).unwrap(), epoch, Some(&work))
        .unwrap();
    let before = authority.ownership_report();
    let discovered = authority.outstanding_close_owners();
    for stream in 2..=3 {
        assert_eq!(
            authority
                .mandatory_close(
                    &mut turn,
                    StreamId::new(stream).unwrap(),
                    epoch,
                    Some(&work)
                )
                .unwrap_err(),
            AuthorityError::InvalidOwner
        );
        assert_eq!(authority.ownership_report(), before);
        assert_eq!(authority.outstanding_close_owners(), discovered);
    }
    for stream in 2..=3 {
        let own = handle
            .reserve_work(&mut turn, WorkKind::PendingPlan)
            .unwrap();
        let owner = authority
            .mandatory_close(&mut turn, StreamId::new(stream).unwrap(), epoch, Some(&own))
            .unwrap();
        let command = lease(&authority, &mut turn, &owner).into_command();
        assert!(matches!(
            authority.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
            DispatchReport::Dispatched
        ));
    }
    let command = lease(&authority, &mut turn, &rightful).into_command();
    assert!(matches!(
        authority.dispatch(&mut turn, command, |_| Ok::<_, ()>(())),
        DispatchReport::Dispatched
    ));
    let report = authority.ownership_report();
    assert_eq!(report.work_used, 1);
    assert_eq!(report.reserved_scopes, 3);
    assert!(report.work_used + report.reserved_scopes + report.reserved_archive <= 13);
    assert!(
        report.metadata_backing_bytes + report.inline_accounted_capacity_bytes
            <= report.metadata_ceiling_bytes
    );
    assert!(report.inline_accounted_capacity_bytes <= report.inline_ceiling_bytes);
}
