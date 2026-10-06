use super::{VerifiedFrameProof, VerifiedProofConflict, VerifiedWarmupProof};
use crate::{
    BitgetMessage, BookFrameObservation, BookInvalidReason, BookValidity, ContinuityOutcome,
    ContinuityRule, DataHealthReducer, HealthDiagnostic, HealthEffect, HealthError,
    HealthObservation, PendingLimit, RecordedHealthObservation, StepResult, decode_message,
};
use domain::artifact::ArtifactRef;
use domain::event::{ClockScope, MonotonicSample};
use domain::identity::{
    BookEpoch, BookId, CaptureSessionId, Channel, ClockId, ConnectionEpoch, ConnectionId, EpochTag,
    FeedProfileVersion, IdentityError, InstrumentRef, InstrumentSlot, MarketKind, MonotonicNs,
    RecordNo, SpecRef, SpecVersion, StreamBinding, StreamId, SubscriptionEpoch, Token,
};
use domain::policy::{DurabilityMode, HealthPolicy, PolicyFields, RecordingGate, SilenceRule};
use domain::record::{
    BookEvidenceKind, EpochChange, Freshness, GapScope, GapTarget, Reason, Transport,
    VerificationEvidence, WarmupEvidence,
};

const SNAPSHOT: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-snapshot.json");
const UPDATE: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-update.json");
const GAP: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-gap.json");
const DUPLICATE: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-duplicate.json");
const RESET: &[u8] = include_bytes!("../../../tests/fixtures/bitget/books50-reset.json");
const EMPTY_LEVELS: &[u8] =
    include_bytes!("../../../tests/fixtures/bitget/books50-empty-levels.json");

fn books(bytes: &[u8]) -> crate::Books50Frame {
    match decode_message(bytes).expect("accepted books50 fixture must decode") {
        BitgetMessage::Books50(frame) => frame,
        BitgetMessage::PublicTrade(_) => panic!("expected books50 frame"),
    }
}

fn clock() -> ClockScope {
    ClockScope {
        session: CaptureSessionId::new([1; 16]).expect("nonzero session"),
        clock: ClockId::new(1).expect("nonzero clock"),
    }
}

fn policy() -> HealthPolicy {
    HealthPolicy {
        fields: PolicyFields {
            silence_rule: SilenceRule::StaleAfterDeadline,
            freshness_deadline_ns: Some(10),
            warmup_min_updates: Some(2),
            warmup_min_elapsed_ns: Some(5),
            allow_quiet_with_proof: false,
            require_two_sided_snapshot: false,
            recording_gate: RecordingGate::Written,
        },
        pending_max_frames: 4,
        pending_max_raw_bytes: 4096,
        pending_max_outputs: 16,
        pending_wait_ns: 100,
        quiet_max_lifetime_ns: None,
    }
}

fn reducer(policy: HealthPolicy) -> DataHealthReducer {
    DataHealthReducer::new(clock(), policy, DurabilityMode::Buffered)
        .expect("test policy must be valid")
}

fn instrument(symbol: &str) -> InstrumentRef {
    InstrumentRef {
        venue: Token::new("bitget").expect("venue"),
        market: MarketKind::Perpetual,
        product_namespace: Token::new("usdt-futures").expect("namespace"),
        native_symbol: Token::new(symbol).expect("symbol"),
    }
}

fn binding(stream: u32, slot: u32, book: u32, connection: u32, symbol: &str) -> StreamBinding {
    let spec_version = SpecVersion::new(1).expect("spec");
    StreamBinding {
        id: StreamId::new(stream).expect("stream"),
        instrument_slot: InstrumentSlot::new(slot).expect("slot"),
        spec: SpecRef {
            instrument: instrument(symbol),
            version: spec_version,
        },
        connection_id: ConnectionId::new(connection).expect("connection"),
        channel: Channel::BookNormal,
        book_id: Some(BookId::new(book).expect("book")),
        tag: EpochTag {
            spec: spec_version,
            connection: ConnectionEpoch::new(1).expect("connection epoch"),
            subscription: SubscriptionEpoch::new(1).expect("subscription epoch"),
            book: Some(BookEpoch::new(1).expect("book epoch")),
        },
        feed_profile: FeedProfileVersion::new(1).expect("profile"),
    }
}

fn proof() -> ArtifactRef {
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        .parse()
        .expect("synthetic proof ref")
}

fn recorded(record: u64, sample_ns: u64, value: HealthObservation) -> RecordedHealthObservation {
    RecordedHealthObservation {
        record: RecordNo::new(record).expect("record"),
        sample: MonotonicSample {
            scope: clock(),
            ns: MonotonicNs::new(sample_ns),
        },
        value,
    }
}

fn frame(binding: &StreamBinding, value: crate::Books50Frame) -> HealthObservation {
    frame_with_bounds(binding, value, 256, 1)
}

fn frame_with_bounds(
    binding: &StreamBinding,
    value: crate::Books50Frame,
    raw_bytes: u32,
    candidate_outputs: u32,
) -> HealthObservation {
    HealthObservation::BookFrame(BookFrameObservation {
        stream: binding.id,
        tag: binding.tag,
        raw_bytes,
        candidate_outputs,
        frame: value,
    })
}

fn verify(binding: &StreamBinding, raw: u64, kind: BookEvidenceKind) -> HealthObservation {
    verify_until(binding, raw, kind, None)
}

fn verify_until(
    binding: &StreamBinding,
    raw: u64,
    kind: BookEvidenceKind,
    valid_until_ns: Option<u64>,
) -> HealthObservation {
    HealthObservation::VerifiedFrame(VerifiedFrameProof::accepted(
        VerificationEvidence {
            stream: binding.id,
            tag: binding.tag,
            raw: RecordNo::new(raw).expect("raw"),
            kind,
            profile: binding.feed_profile,
            proof: proof(),
        },
        valid_until_ns,
    ))
}

fn warmup(
    binding: &StreamBinding,
    anchor: u64,
    updates: u32,
    elapsed_ns: u64,
) -> HealthObservation {
    HealthObservation::VerifiedWarmup(VerifiedWarmupProof::accepted(WarmupEvidence {
        stream: binding.id,
        tag: binding.tag,
        anchor: RecordNo::new(anchor).expect("anchor"),
        update_count: updates,
        elapsed_ns,
        proof: proof(),
    }))
}

fn register_and_up(reducer: &mut DataHealthReducer, binding: &StreamBinding) {
    reducer
        .step(recorded(
            1,
            0,
            HealthObservation::RegisterStream(binding.clone()),
        ))
        .expect("register");
    reducer
        .step(recorded(
            2,
            0,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: binding.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("transport up");
}

#[test]
fn gap_barrier_blocks_old_proof_and_repeated_snapshot_is_conservative() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 0, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot raw");
    runtime
        .step(recorded(
            4,
            0,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("snapshot proof");
    assert_eq!(
        runtime.stream_state(binding.id).expect("state").book,
        Some(BookValidity::Warming)
    );

    let gap = HealthObservation::Gap {
        scope: GapScope::ExplicitTargets(vec![GapTarget {
            stream: binding.id,
            tag: binding.tag,
            range: None,
            loss_count: None,
        }]),
        reason: Reason::SourceGap,
    };
    runtime.step(recorded(5, 1, gap)).expect("gap");
    let invalid = runtime.stream_state(binding.id).expect("state");
    assert_eq!(invalid.barrier.get(), 5);
    assert_eq!(invalid.freshness, Freshness::Unknown);
    assert_eq!(invalid.pending.frames, 0);
    assert_eq!(invalid.anchor, None);
    assert_eq!(
        invalid.book,
        Some(BookValidity::Invalid(BookInvalidReason::Gap(
            Reason::SourceGap
        )))
    );

    let old_proof = runtime
        .step(recorded(
            6,
            1,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("old proof is diagnostic");
    assert_eq!(
        old_proof.diagnostics,
        vec![HealthDiagnostic::PreBarrier {
            stream: binding.id,
            referenced: RecordNo::new(3).expect("raw"),
            barrier: RecordNo::new(5).expect("barrier"),
        }]
    );
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .barrier
            .get(),
        5
    );

    let mut new_snapshot = books(SNAPSHOT);
    new_snapshot.seq = 2_000;
    runtime
        .step(recorded(7, 6, frame(&binding, new_snapshot.clone())))
        .expect("post-barrier snapshot");
    runtime
        .step(recorded(
            8,
            6,
            verify(&binding, 7, BookEvidenceKind::Snapshot),
        ))
        .expect("post-barrier proof");
    let recovered = runtime.stream_state(binding.id).expect("state");
    assert_eq!(recovered.book, Some(BookValidity::Warming));
    assert_eq!(recovered.anchor.expect("anchor").get(), 7);
    assert!(!runtime.usable_data(binding.id));

    let repeated = runtime
        .step(recorded(9, 6, frame(&binding, new_snapshot)))
        .expect("repeated snapshot");
    assert!(matches!(
        repeated.continuity.as_slice(),
        [crate::ContinuityReport {
            outcome: ContinuityOutcome::UnexpectedSnapshot { .. },
            ..
        }]
    ));
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 9);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::UnexpectedSnapshot))
    );
    assert!(!runtime.usable_data(binding.id));
}

#[test]
fn source_gap_and_reset_discontinuity_fail_closed() {
    for (fault_frame, expected) in [
        (books(GAP), BookInvalidReason::ContinuityGap),
        (books(RESET), BookInvalidReason::ResetOrDiscontinuity),
    ] {
        let binding = binding(1, 1, 1, 1, "BTCUSDT");
        let mut runtime = reducer(policy());
        register_and_up(&mut runtime, &binding);

        runtime
            .step(recorded(3, 0, frame(&binding, books(SNAPSHOT))))
            .expect("snapshot");
        runtime
            .step(recorded(
                4,
                0,
                verify(&binding, 3, BookEvidenceKind::Snapshot),
            ))
            .expect("snapshot proof");
        runtime
            .step(recorded(5, 2, frame(&binding, books(UPDATE))))
            .expect("update");
        runtime
            .step(recorded(6, 2, verify(&binding, 5, BookEvidenceKind::Delta)))
            .expect("update proof");

        runtime
            .step(recorded(7, 3, frame(&binding, fault_frame)))
            .expect("continuity fault");
        let state = runtime.stream_state(binding.id).expect("state");
        assert_eq!(state.barrier.get(), 7);
        assert_eq!(state.pending.frames, 0);
        assert_eq!(state.anchor, None);
        assert_eq!(state.freshness, Freshness::Unknown);
        assert_eq!(state.book, Some(BookValidity::Invalid(expected)));
    }
}

#[test]
fn pending_overflow_is_typed_fail_closed_without_eviction() {
    let mut bounded = policy();
    bounded.pending_max_frames = 1;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(bounded);
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 0, frame(&binding, books(SNAPSHOT))))
        .expect("first pending frame");
    let before = runtime.stream_state(binding.id).expect("state");
    assert_eq!(before.pending.frames, 1);
    assert_eq!(before.pending.raw_bytes, 256);

    let result = runtime
        .step(recorded(4, 1, frame(&binding, books(UPDATE))))
        .expect("overflow is semantic outcome");
    assert_eq!(
        result.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::PendingOverflow(PendingLimit::Frames),
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 4);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.pending.raw_bytes, 0);
    assert_eq!(state.pending.outputs, 0);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::PendingOverflow(
            PendingLimit::Frames
        )))
    );
}

#[test]
fn duplicate_is_noop_but_current_scope_conflict_invalidates() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 0, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot");
    runtime
        .step(recorded(
            4,
            0,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("snapshot proof");
    runtime
        .step(recorded(5, 2, frame(&binding, books(UPDATE))))
        .expect("update");
    runtime
        .step(recorded(6, 2, verify(&binding, 5, BookEvidenceKind::Delta)))
        .expect("update proof");

    let before = runtime.stream_state(binding.id).expect("state");
    let duplicate = runtime
        .step(recorded(7, 2, frame(&binding, books(DUPLICATE))))
        .expect("duplicate");
    assert!(duplicate.effects.is_empty());
    assert!(matches!(
        duplicate.continuity.as_slice(),
        [crate::ContinuityReport {
            outcome: ContinuityOutcome::DuplicateDiagnostic { .. },
            ..
        }]
    ));
    assert_eq!(
        duplicate.diagnostics,
        vec![HealthDiagnostic::DuplicateObservation {
            stream: binding.id,
            raw: RecordNo::new(7).expect("raw"),
        }]
    );
    assert_eq!(runtime.stream_state(binding.id).expect("state"), before);

    runtime
        .step(recorded(
            8,
            2,
            HealthObservation::ProofConflict(VerifiedProofConflict::accepted(
                binding.id,
                binding.tag,
                RecordNo::new(5).expect("raw"),
            )),
        ))
        .expect("conflicting repeat");
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 8);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::ProofConflict))
    );
}

#[test]
fn shared_connection_registration_and_down_fanout_preserve_isolation() {
    let a = binding(1, 1, 1, 1, "BTCUSDT");
    let b = binding(2, 2, 2, 1, "ETHUSDT");
    let c = binding(3, 3, 3, 2, "SOLUSDT");
    let mut runtime = reducer(policy());

    register_and_up(&mut runtime, &a);
    runtime
        .step(recorded(3, 0, frame(&a, books(SNAPSHOT))))
        .expect("a snapshot");
    runtime
        .step(recorded(4, 0, verify(&a, 3, BookEvidenceKind::Snapshot)))
        .expect("a snapshot proof");
    let a_before_b = runtime.stream_state(a.id).expect("a state");

    runtime
        .step(recorded(5, 0, HealthObservation::RegisterStream(b.clone())))
        .expect("register b on shared connection");
    assert_eq!(runtime.stream_state(a.id).expect("a state"), a_before_b);
    let b_state = runtime.stream_state(b.id).expect("b state");
    assert_eq!(b_state.transport, Transport::Up);
    assert_eq!(b_state.freshness, Freshness::Unknown);
    assert_eq!(b_state.book, Some(BookValidity::NoSnapshot));

    runtime
        .step(recorded(6, 0, HealthObservation::RegisterStream(c.clone())))
        .expect("register c");
    runtime
        .step(recorded(
            7,
            0,
            HealthObservation::Transport {
                connection: c.connection_id,
                epoch: c.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("c transport up");
    runtime
        .step(recorded(8, 0, frame(&c, books(SNAPSHOT))))
        .expect("c snapshot");
    runtime
        .step(recorded(9, 0, verify(&c, 8, BookEvidenceKind::Snapshot)))
        .expect("c proof");
    let c_before_down = runtime.stream_state(c.id).expect("c state");

    runtime
        .step(recorded(
            10,
            1,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: a.tag.connection,
                value: Transport::Down,
            },
        ))
        .expect("shared connection down");
    for stream in [a.id, b.id] {
        let state = runtime.stream_state(stream).expect("dependent");
        assert_eq!(state.transport, Transport::Down);
        assert_eq!(state.barrier.get(), 10);
        assert_eq!(
            state.book,
            Some(BookValidity::Invalid(BookInvalidReason::TransportDown))
        );
    }
    assert_eq!(runtime.stream_state(c.id).expect("c state"), c_before_down);

    runtime
        .step(recorded(
            11,
            1,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: a.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("connection back up");
    for stream in [a.id, b.id] {
        let state = runtime.stream_state(stream).expect("dependent");
        assert_eq!(state.transport, Transport::Up);
        assert_eq!(
            state.book,
            Some(BookValidity::Invalid(BookInvalidReason::TransportDown))
        );
        assert_eq!(state.barrier.get(), 10);
    }

    let heartbeat = runtime
        .step(recorded(
            12,
            1,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: a.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("idempotent heartbeat/up");
    assert!(heartbeat.effects.is_empty());
    assert_eq!(
        heartbeat.diagnostics,
        vec![HealthDiagnostic::DuplicateTransport {
            connection: a.connection_id,
            epoch: a.tag.connection,
            value: Transport::Up,
        }]
    );
}

#[test]
fn connection_epoch_advance_resets_only_dependents_to_unknown_generation() {
    let a = binding(1, 1, 1, 1, "BTCUSDT");
    let b = binding(2, 2, 2, 1, "ETHUSDT");
    let c = binding(3, 3, 3, 2, "SOLUSDT");
    let old_a_tag = a.tag;
    let mut runtime = reducer(policy());

    runtime
        .step(recorded(1, 0, HealthObservation::RegisterStream(a.clone())))
        .expect("register a");
    runtime
        .step(recorded(2, 0, HealthObservation::RegisterStream(b.clone())))
        .expect("register b");
    runtime
        .step(recorded(
            3,
            0,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: a.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("conn1 up");
    runtime
        .step(recorded(4, 0, HealthObservation::RegisterStream(c.clone())))
        .expect("register c");
    runtime
        .step(recorded(
            5,
            0,
            HealthObservation::Transport {
                connection: c.connection_id,
                epoch: c.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("conn2 up");
    runtime
        .step(recorded(6, 0, frame(&a, books(SNAPSHOT))))
        .expect("a snapshot");
    runtime
        .step(recorded(7, 0, verify(&a, 6, BookEvidenceKind::Snapshot)))
        .expect("a proof");
    runtime
        .step(recorded(8, 0, frame(&c, books(SNAPSHOT))))
        .expect("c snapshot");
    runtime
        .step(recorded(9, 0, verify(&c, 8, BookEvidenceKind::Snapshot)))
        .expect("c proof");
    let c_before = runtime.stream_state(c.id).expect("c state");

    let next_epoch = ConnectionEpoch::new(2).expect("epoch");
    runtime
        .step(recorded(
            10,
            1,
            HealthObservation::EpochAdvance(EpochChange::Connection {
                owner: a.connection_id,
                expected: a.tag.connection,
                next: next_epoch,
            }),
        ))
        .expect("connection epoch advance");

    for stream in [a.id, b.id] {
        let state = runtime.stream_state(stream).expect("dependent");
        assert_eq!(state.binding.tag.connection, next_epoch);
        assert_eq!(state.transport, Transport::Unknown);
        assert_eq!(state.freshness, Freshness::Unknown);
        assert_eq!(state.book, Some(BookValidity::NoSnapshot));
        assert_eq!(state.barrier.get(), 10);
    }
    assert_eq!(runtime.stream_state(c.id).expect("c state"), c_before);

    let obsolete = runtime
        .step(recorded(
            11,
            1,
            HealthObservation::BookFrame(BookFrameObservation {
                stream: a.id,
                tag: old_a_tag,
                raw_bytes: 256,
                candidate_outputs: 1,
                frame: books(SNAPSHOT),
            }),
        ))
        .expect("old epoch raw is diagnostic");
    assert!(obsolete.effects.is_empty());
    assert_eq!(
        obsolete.diagnostics,
        vec![HealthDiagnostic::ObsoleteScope { stream: a.id }]
    );
    let after_old = runtime.stream_state(a.id).expect("a state");
    assert_eq!(after_old.barrier.get(), 10);
    assert_eq!(after_old.book, Some(BookValidity::NoSnapshot));

    runtime
        .step(recorded(
            12,
            1,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: next_epoch,
                value: Transport::Up,
            },
        ))
        .expect("new generation up");
    let state = runtime.stream_state(a.id).expect("a state");
    assert_eq!(state.transport, Transport::Up);
    assert_eq!(state.book, Some(BookValidity::NoSnapshot));
    assert_eq!(state.barrier.get(), 10);
}

#[test]
fn subscription_and_book_epoch_advances_are_owner_isolated() {
    let a = binding(1, 1, 1, 1, "BTCUSDT");
    let b = binding(2, 2, 2, 1, "ETHUSDT");
    let mut runtime = reducer(policy());

    runtime
        .step(recorded(1, 0, HealthObservation::RegisterStream(a.clone())))
        .expect("register a");
    runtime
        .step(recorded(2, 0, HealthObservation::RegisterStream(b.clone())))
        .expect("register b");
    runtime
        .step(recorded(
            3,
            0,
            HealthObservation::Transport {
                connection: a.connection_id,
                epoch: a.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("shared up");
    let b_before = runtime.stream_state(b.id).expect("b state");

    let next_subscription = SubscriptionEpoch::new(2).expect("subscription epoch");
    runtime
        .step(recorded(
            4,
            1,
            HealthObservation::EpochAdvance(EpochChange::Subscription {
                owner: a.id,
                expected: a.tag.subscription,
                next: next_subscription,
            }),
        ))
        .expect("subscription advance");
    let a_after_subscription = runtime.stream_state(a.id).expect("a state");
    assert_eq!(
        a_after_subscription.binding.tag.subscription,
        next_subscription
    );
    assert_eq!(a_after_subscription.barrier.get(), 4);
    assert_eq!(a_after_subscription.transport, Transport::Up);
    assert_eq!(runtime.stream_state(b.id).expect("b state"), b_before);

    let next_book = BookEpoch::new(2).expect("book epoch");
    runtime
        .step(recorded(
            5,
            1,
            HealthObservation::EpochAdvance(EpochChange::Book {
                owner: b.book_id.expect("book"),
                expected: b.tag.book.expect("book epoch"),
                next: next_book,
            }),
        ))
        .expect("book advance");
    let b_after = runtime.stream_state(b.id).expect("b state");
    assert_eq!(b_after.binding.tag.book, Some(next_book));
    assert_eq!(b_after.barrier.get(), 5);
    assert_eq!(b_after.transport, Transport::Up);
    assert_eq!(
        runtime.stream_state(a.id).expect("a state"),
        a_after_subscription
    );
}

#[test]
fn continuity_rules_warmup_and_recorded_order_ignore_source_timestamp_order() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 0, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot");
    runtime
        .step(recorded(
            4,
            0,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("snapshot proof");

    let mut first = books(UPDATE);
    first.pseq = 999;
    first.seq = 1_005;
    first.source_timestamp.value = 300;
    first.source_timestamp.lexical = "300".to_owned();
    let first_result = runtime
        .step(recorded(5, 2, frame(&binding, first)))
        .expect("first update");
    assert_eq!(
        first_result.continuity[0].outcome,
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::SnapshotInterval,
            previous_seq: 1_000,
            current_pseq: 999,
            current_seq: 1_005,
        }
    );
    runtime
        .step(recorded(6, 2, verify(&binding, 5, BookEvidenceKind::Delta)))
        .expect("first update proof");
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.book, Some(BookValidity::Warming));
    assert_eq!(state.progress, 1);
    assert!(!runtime.usable_data(binding.id));

    let mut second = books(EMPTY_LEVELS);
    second.pseq = 1_005;
    second.seq = 1_006;
    second.source_timestamp.value = 100;
    second.source_timestamp.lexical = "100".to_owned();
    let second_result = runtime
        .step(recorded(7, 5, frame(&binding, second.clone())))
        .expect("second update");
    assert_eq!(
        second_result.continuity[0].outcome,
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::PreviousSeqEqualsPseq,
            previous_seq: 1_005,
            current_pseq: 1_005,
            current_seq: 1_006,
        }
    );
    runtime
        .step(recorded(8, 5, verify(&binding, 7, BookEvidenceKind::Delta)))
        .expect("second update proof");

    let before_witness = runtime.stream_state(binding.id).expect("state");
    assert_eq!(before_witness.progress, 2);
    assert_eq!(before_witness.book, Some(BookValidity::Warming));
    assert!(!runtime.usable_data(binding.id));

    runtime
        .step(recorded(9, 5, warmup(&binding, 3, 2, 5)))
        .expect("warmup witness");
    assert_eq!(
        runtime.stream_state(binding.id).expect("state").book,
        Some(BookValidity::Usable)
    );
    assert!(runtime.usable_data(binding.id));

    let mut third = second;
    third.pseq = 1_006;
    third.seq = 1_007;
    third.source_timestamp.value = 100;
    third.source_timestamp.lexical = "100".to_owned();
    let third_result = runtime
        .step(recorded(10, 4, frame(&binding, third)))
        .expect("equal source timestamp and reversed recorded sample");
    assert_eq!(
        third_result.continuity[0].outcome,
        ContinuityOutcome::Continuous {
            rule: ContinuityRule::PreviousSeqEqualsPseq,
            previous_seq: 1_006,
            current_pseq: 1_006,
            current_seq: 1_007,
        }
    );
    runtime
        .step(recorded(
            11,
            4,
            verify(&binding, 10, BookEvidenceKind::Delta),
        ))
        .expect("third update proof");
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(runtime.evaluation_ns(), 5);
    assert_eq!(state.last_applied_raw.expect("last raw").get(), 10);
    assert_eq!(state.last_valid_sample_ns, Some(5));
    assert_eq!(state.progress, 2);
    assert_eq!(state.book, Some(BookValidity::Usable));
    assert!(runtime.usable_data(binding.id));

    runtime
        .step(recorded(
            12,
            15,
            HealthObservation::Timer { stream: binding.id },
        ))
        .expect("silence timer");
    let stale = runtime.stream_state(binding.id).expect("state");
    assert_eq!(stale.freshness, Freshness::Stale);
    assert_eq!(stale.transport, Transport::Up);
    assert_eq!(stale.book, Some(BookValidity::Usable));
    assert!(!runtime.usable_data(binding.id));
}

#[test]
fn same_bounded_trace_is_deterministic_across_repeated_runs() {
    let mut bounded = policy();
    bounded.pending_max_frames = 2;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");

    let trace = vec![
        recorded(1, 0, HealthObservation::RegisterStream(binding.clone())),
        recorded(
            2,
            0,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: binding.tag.connection,
                value: Transport::Up,
            },
        ),
        recorded(3, 0, frame(&binding, books(SNAPSHOT))),
        recorded(4, 1, frame(&binding, books(UPDATE))),
        recorded(5, 2, frame(&binding, books(EMPTY_LEVELS))),
    ];

    fn run(
        policy: HealthPolicy,
        trace: &[RecordedHealthObservation],
    ) -> (Vec<StepResult>, crate::DataHealthSnapshot) {
        let mut runtime = reducer(policy);
        let results = trace
            .iter()
            .cloned()
            .map(|observation| runtime.step(observation).expect("trace step"))
            .collect();
        (results, runtime.snapshot())
    }

    let first = run(bounded, &trace);
    let second = run(bounded, &trace);
    assert_eq!(first, second);

    let final_state = first
        .1
        .streams
        .iter()
        .find(|state| state.binding.id == binding.id)
        .expect("stream");
    assert_eq!(final_state.pending.frames, 0);
    assert_eq!(final_state.pending.raw_bytes, 0);
    assert_eq!(final_state.pending.outputs, 0);
    assert_eq!(final_state.barrier.get(), 5);
    assert_eq!(
        final_state.book,
        Some(BookValidity::Invalid(BookInvalidReason::PendingOverflow(
            PendingLimit::Frames
        )))
    );
}

#[test]
fn proof_expiry_is_checked_at_receipt_and_again_at_ordered_release() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 10, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot pending");
    runtime
        .step(recorded(4, 11, frame(&binding, books(UPDATE))))
        .expect("delta pending");
    let ready_delta = runtime
        .step(recorded(
            5,
            12,
            verify_until(&binding, 4, BookEvidenceKind::Delta, Some(13)),
        ))
        .expect("later proof becomes ready");
    assert!(ready_delta.effects.is_empty());
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .pending
            .frames,
        2
    );

    let release = runtime
        .step(recorded(
            6,
            13,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("ordered release fails closed on expired ready delta");
    assert!(release.effects.iter().any(|effect| matches!(
        effect,
        HealthEffect::SnapshotReleased { stream, raw }
            if *stream == binding.id && raw.get() == 3
    )));
    assert!(!release.effects.iter().any(|effect| matches!(
        effect,
        HealthEffect::UpdateReleased { stream, raw, .. }
            if *stream == binding.id && raw.get() == 4
    )));
    assert_eq!(
        release.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(6).expect("barrier"),
            reason: BookInvalidReason::ProofExpired,
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 6);
    assert_eq!(state.freshness, Freshness::Unknown);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::ProofExpired))
    );
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.anchor, None);
    assert_eq!(state.progress, 0);
    assert_eq!(state.witness, None);
    assert_eq!(state.last_valid_sample_ns, None);
    assert!(!runtime.usable_data(binding.id));

    let mut expired_at_receipt = reducer(policy());
    register_and_up(&mut expired_at_receipt, &binding);
    expired_at_receipt
        .step(recorded(3, 10, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot pending");
    let receipt = expired_at_receipt
        .step(recorded(
            4,
            13,
            verify_until(&binding, 3, BookEvidenceKind::Snapshot, Some(13)),
        ))
        .expect("expired proof is semantic invalidation");
    assert!(
        !receipt
            .effects
            .iter()
            .any(|effect| matches!(effect, HealthEffect::SnapshotReleased { .. }))
    );
    assert_eq!(
        receipt.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::ProofExpired,
        })
    );
    let state = expired_at_receipt
        .stream_state(binding.id)
        .expect("state after expired receipt");
    assert_eq!(state.barrier.get(), 4);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.anchor, None);
    assert_eq!(state.freshness, Freshness::Unknown);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::ProofExpired))
    );
}

#[test]
fn freshness_deadline_overflow_is_unknown_with_typed_diagnostic_only() {
    let mut overflow_policy = policy();
    overflow_policy.pending_wait_ns = 1;
    overflow_policy.fields.freshness_deadline_ns = Some(10);
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(overflow_policy);
    register_and_up(&mut runtime, &binding);

    let sample = u64::MAX - 5;
    runtime
        .step(recorded(3, sample, frame(&binding, books(SNAPSHOT))))
        .expect("snapshot pending");
    let release = runtime
        .step(recorded(
            4,
            sample,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("snapshot release");
    assert_eq!(
        release.diagnostics,
        vec![HealthDiagnostic::FreshnessDeadlineOverflow { stream: binding.id }]
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.transport, Transport::Up);
    assert_eq!(state.freshness, Freshness::Unknown);
    assert_eq!(state.book, Some(BookValidity::Warming));
    assert_eq!(state.barrier.get(), 1);
    assert_eq!(state.anchor.expect("anchor").get(), 3);
    assert_eq!(state.last_valid_sample_ns, Some(sample));
    assert!(!runtime.usable_data(binding.id));
}

#[test]
fn pending_raw_bytes_overflow_is_typed_and_clears_pending() {
    let mut bounded = policy();
    bounded.pending_max_raw_bytes = 300;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(bounded);
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(
            3,
            0,
            frame_with_bounds(&binding, books(SNAPSHOT), 200, 1),
        ))
        .expect("first pending frame");
    let overflow = runtime
        .step(recorded(
            4,
            1,
            frame_with_bounds(&binding, books(UPDATE), 200, 1),
        ))
        .expect("raw-byte overflow is semantic");
    assert_eq!(
        overflow.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::PendingOverflow(PendingLimit::RawBytes),
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.pending.raw_bytes, 0);
    assert_eq!(state.pending.outputs, 0);
}

#[test]
fn pending_output_count_overflow_is_typed_and_clears_pending() {
    let mut bounded = policy();
    bounded.pending_max_outputs = 1;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(bounded);
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(
            3,
            0,
            frame_with_bounds(&binding, books(SNAPSHOT), 100, 1),
        ))
        .expect("first pending frame");
    let overflow = runtime
        .step(recorded(
            4,
            1,
            frame_with_bounds(&binding, books(UPDATE), 100, 1),
        ))
        .expect("output-count overflow is semantic");
    assert_eq!(
        overflow.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::PendingOverflow(PendingLimit::Outputs),
        })
    );
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .pending
            .frames,
        0
    );
}

#[test]
fn pending_deadline_equality_times_out_before_current_observation() {
    let mut bounded = policy();
    bounded.pending_wait_ns = 10;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(bounded);
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(3, 5, frame(&binding, books(SNAPSHOT))))
        .expect("pending snapshot");
    let timeout = runtime
        .step(recorded(
            4,
            15,
            HealthObservation::Timer { stream: binding.id },
        ))
        .expect("deadline equality expires before timer dispatch");
    assert_eq!(
        timeout.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::PendingTimeout,
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 4);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.freshness, Freshness::Unknown);
}

#[test]
fn pending_deadline_arithmetic_overflow_fails_closed_without_wrap() {
    let mut bounded = policy();
    bounded.pending_wait_ns = 10;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(bounded);
    register_and_up(&mut runtime, &binding);

    let overflow = runtime
        .step(recorded(3, u64::MAX - 5, frame(&binding, books(SNAPSHOT))))
        .expect("deadline arithmetic overflow is semantic");
    assert_eq!(
        overflow.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(3).expect("barrier"),
            reason: BookInvalidReason::PendingDeadlineOverflow,
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 3);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.anchor, None);
    assert_eq!(state.freshness, Freshness::Unknown);
}

#[test]
fn second_writer_for_same_book_ref_is_rejected_transactionally() {
    let first = binding(1, 1, 1, 1, "BTCUSDT");
    let second = binding(2, 1, 2, 1, "BTCUSDT");
    let mut runtime = reducer(policy());

    runtime
        .step(recorded(
            1,
            0,
            HealthObservation::RegisterStream(first.clone()),
        ))
        .expect("first writer");
    let before = runtime.stream_state(first.id).expect("first state");

    let error = runtime
        .step(recorded(
            2,
            0,
            HealthObservation::RegisterStream(second.clone()),
        ))
        .expect_err("same BookRef must require a new archive");
    assert_eq!(
        error,
        HealthError::Identity(IdentityError::WriterRebindRequiresNewArchive)
    );
    assert_eq!(
        runtime.last_record(),
        Some(RecordNo::new(1).expect("record"))
    );
    assert_eq!(runtime.stream_state(first.id).expect("first state"), before);
    assert!(runtime.stream_state(second.id).is_none());
}

#[test]
fn repeated_current_down_advances_barrier_and_blocks_intervening_pending_frame() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    register_and_up(&mut runtime, &binding);

    runtime
        .step(recorded(
            3,
            1,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: binding.tag.connection,
                value: Transport::Down,
            },
        ))
        .expect("first down");
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .barrier
            .get(),
        3
    );

    runtime
        .step(recorded(4, 2, frame(&binding, books(SNAPSHOT))))
        .expect("post-first-down snapshot may be pending");
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .pending
            .frames,
        1
    );

    let second_down = runtime
        .step(recorded(
            5,
            3,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: binding.tag.connection,
                value: Transport::Down,
            },
        ))
        .expect("second current down must invalidate again");
    assert_eq!(
        second_down.diagnostics,
        vec![HealthDiagnostic::DuplicateTransport {
            connection: binding.connection_id,
            epoch: binding.tag.connection,
            value: Transport::Down,
        }]
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 5);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(BookInvalidReason::TransportDown))
    );

    runtime
        .step(recorded(
            6,
            3,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: binding.tag.connection,
                value: Transport::Up,
            },
        ))
        .expect("up after second down");

    let proof = runtime
        .step(recorded(
            7,
            3,
            verify(&binding, 4, BookEvidenceKind::Snapshot),
        ))
        .expect("intervening raw is pre-barrier");
    assert_eq!(
        proof.diagnostics,
        vec![HealthDiagnostic::PreBarrier {
            stream: binding.id,
            referenced: RecordNo::new(4).expect("raw"),
            barrier: RecordNo::new(5).expect("barrier"),
        }]
    );
    assert_eq!(
        runtime
            .stream_state(binding.id)
            .expect("state")
            .barrier
            .get(),
        5
    );
}

#[test]
fn two_sided_snapshot_policy_fails_closed_without_quantity_semantics() {
    let mut guarded = policy();
    guarded.fields.require_two_sided_snapshot = true;
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(guarded);
    register_and_up(&mut runtime, &binding);

    let mut one_sided = books(SNAPSHOT);
    one_sided.asks.clear();
    runtime
        .step(recorded(3, 1, frame(&binding, one_sided)))
        .expect("one-sided snapshot pending");
    let proof = runtime
        .step(recorded(
            4,
            1,
            verify(&binding, 3, BookEvidenceKind::Snapshot),
        ))
        .expect("two-sided guard is semantic invalidation");

    assert!(
        !proof
            .effects
            .iter()
            .any(|effect| matches!(effect, HealthEffect::SnapshotReleased { .. }))
    );
    assert_eq!(
        proof.effects.last(),
        Some(&HealthEffect::StreamInvalidated {
            stream: binding.id,
            barrier: RecordNo::new(4).expect("barrier"),
            reason: BookInvalidReason::SnapshotNotTwoSided,
        })
    );
    let state = runtime.stream_state(binding.id).expect("state");
    assert_eq!(state.barrier.get(), 4);
    assert_eq!(state.anchor, None);
    assert_eq!(state.pending.frames, 0);
    assert_eq!(state.freshness, Freshness::Unknown);
    assert_eq!(
        state.book,
        Some(BookValidity::Invalid(
            BookInvalidReason::SnapshotNotTwoSided
        ))
    );
    assert!(!runtime.usable_data(binding.id));
}

#[test]
fn future_unannounced_connection_epoch_is_transactional_mismatch() {
    let binding = binding(1, 1, 1, 1, "BTCUSDT");
    let mut runtime = reducer(policy());
    runtime
        .step(recorded(
            1,
            0,
            HealthObservation::RegisterStream(binding.clone()),
        ))
        .expect("register");
    let before = runtime.snapshot();

    let future = ConnectionEpoch::new(2).expect("future epoch");
    let error = runtime
        .step(recorded(
            2,
            1,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: future,
                value: Transport::Up,
            },
        ))
        .expect_err("future epoch requires explicit EpochAdvance");
    assert_eq!(error, HealthError::Identity(IdentityError::EpochMismatch));
    assert_eq!(runtime.snapshot(), before);
    assert_eq!(
        runtime.last_record(),
        Some(RecordNo::new(1).expect("record"))
    );

    let old = ConnectionEpoch::new(1).expect("current");
    runtime
        .step(recorded(
            2,
            1,
            HealthObservation::EpochAdvance(EpochChange::Connection {
                owner: binding.connection_id,
                expected: old,
                next: future,
            }),
        ))
        .expect("explicit advance");
    let obsolete = runtime
        .step(recorded(
            3,
            1,
            HealthObservation::Transport {
                connection: binding.connection_id,
                epoch: old,
                value: Transport::Up,
            },
        ))
        .expect("old epoch remains obsolete diagnostic");
    assert_eq!(
        obsolete.diagnostics,
        vec![HealthDiagnostic::ObsoleteConnection {
            connection: binding.connection_id,
            epoch: old,
        }]
    );
}
