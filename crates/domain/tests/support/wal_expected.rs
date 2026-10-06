//! Independently specified DTO expectations for the frozen WAL goldens.
//! No decoder/encoder/seal calculator is used to manufacture these values.

use domain::event::{InputContext, LevelChange, MarketPayload, Side};
use domain::identity::*;
use domain::policy::DurabilityMode;
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;

use super::binary_fixtures::original;
use super::fixtures;

pub fn context(n: u64, bootstrap: bool) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(i64::try_from(n).unwrap()),
        monotonic_ns: MonotonicNs::new(n),
        context: if bootstrap {
            InputContext::Bootstrap
        } else {
            InputContext::Active(fixtures::active(1, 1))
        },
    }
}

pub fn frame(n: u64, segment: u32, value: Record) -> RecordFrame {
    RecordFrame {
        record_no: fixtures::record(n),
        segment_no: SegmentNo::new(segment),
        value,
    }
}

pub fn start() -> RecordFrame {
    frame(
        1,
        0,
        Record::ArchiveStart(ArchiveStart {
            archive: fixtures::archive(),
            session: fixtures::clock().session,
            clock: fixtures::clock().clock,
            mode: DurabilityMode::SyncBeforePublish,
            previous_archive: None,
        }),
    )
}

pub fn numeric() -> NumericSpec {
    NumericSpec::new(NumericSpecFields {
        reference: fixtures::binding(1, Channel::BookNormal).spec,
        price_units: PriceUnits {
            quote: Token::new("USD").unwrap(),
            basis: Token::new("ABC").unwrap(),
        },
        quantity_unit: Token::new("ABC").unwrap(),
        base_asset: Token::new("ABC").unwrap(),
        price_increment: "0.05".parse().unwrap(),
        quantity_increment: "0.001".parse().unwrap(),
        quantity_to_base_multiplier: Some("1".parse().unwrap()),
    })
    .unwrap()
}

pub fn raw(n: u64, segment: u32, attempt: u64) -> RecordFrame {
    frame(
        n,
        segment,
        Record::RawInput(RawInput {
            context: context(n, false),
            stream: StreamId::new(1).unwrap(),
            tag: fixtures::binding(1, Channel::BookNormal).tag,
            attempt: CaptureAttemptNo::new(attempt).unwrap(),
            bytes: format!("snapshot-{n}").into_bytes(),
        }),
    )
}

pub fn gap(n: u64, known: bool) -> RecordFrame {
    frame(
        n,
        0,
        Record::Gap(Gap {
            context: context(n, false),
            scope: GapScope::ExplicitTargets(vec![GapTarget {
                stream: StreamId::new(1).unwrap(),
                tag: fixtures::binding(1, Channel::BookNormal).tag,
                range: known.then(|| {
                    (
                        CaptureAttemptNo::new(2).unwrap(),
                        CaptureAttemptNo::new(2).unwrap(),
                    )
                }),
                loss_count: known.then_some(1),
            }]),
            reason: Reason::QueueOverflow,
        }),
    )
}

pub fn timer(n: u64, segment: u32) -> RecordFrame {
    frame(
        n,
        segment,
        Record::Control(ControlRecord {
            context: context(n, false),
            value: Control::Timer {
                stream: StreamId::new(1).unwrap(),
                timer_id: 1,
                deadline_ns: n,
            },
        }),
    )
}

pub fn single() -> Vec<RecordFrame> {
    vec![
        start(),
        frame(
            2,
            0,
            Record::InstrumentSpec(InstrumentSpecRecord {
                context: context(2, true),
                slot: InstrumentSlot::new(1).unwrap(),
                numeric: numeric(),
                provenance: original("AF-I1").reference,
            }),
        ),
        frame(
            3,
            0,
            Record::StreamDefinition(StreamDefinition {
                context: context(3, true),
                binding: fixtures::binding(1, Channel::BookNormal),
                provenance: original("AF-F1").reference,
            }),
        ),
        frame(
            4,
            0,
            Record::ConfigDefinition(ConfigDefinition {
                context: context(4, true),
                next: fixtures::active(1, 1),
                provenance_kind: ProvenanceKind::Synthetic,
                evidence: original("AF-C1").reference,
                fields: fixtures::policy().fields,
            }),
        ),
        frame(
            5,
            0,
            Record::Control(ControlRecord {
                context: context(5, false),
                value: Control::Transport {
                    connection: ConnectionId::new(1).unwrap(),
                    epoch: ConnectionEpoch::new(1).unwrap(),
                    value: Transport::Up,
                },
            }),
        ),
        raw(6, 0, 1),
        gap(7, true),
        raw(8, 0, 3),
        timer(9, 0),
        frame(
            10,
            0,
            Record::SegmentSeal(SegmentSeal {
                prefix_frame_count: 9,
                prefix_physical_len: 1161,
                prefix_crc32: 1_920_078_791,
                prior_record: fixtures::record(9),
                has_gap: true,
                is_final: true,
            }),
        ),
        frame(
            11,
            0,
            Record::ArchiveSeal(ArchiveSeal {
                expected_segment_count: 1,
                prior_frame_count: 10,
                total_prefix_physical_bytes: 1227,
                prefix_crc32: 1_050_396_281,
                prior_record: fixtures::record(10),
                input_quality: InputQuality::GapsRecorded,
            }),
        ),
    ]
}

pub fn multi() -> (Vec<RecordFrame>, Vec<RecordFrame>) {
    let mut first: Vec<_> = single().into_iter().take(7).collect();
    first.push(frame(
        8,
        0,
        Record::SegmentSeal(SegmentSeal {
            prefix_frame_count: 7,
            prefix_physical_len: 964,
            prefix_crc32: 2_778_686_206,
            prior_record: fixtures::record(7),
            has_gap: true,
            is_final: false,
        }),
    ));
    let second = vec![
        frame(
            9,
            1,
            Record::SegmentStart(SegmentStart {
                archive: fixtures::archive(),
                session: fixtures::clock().session,
                clock: fixtures::clock().clock,
                previous_segment: SegmentNo::new(0),
                previous_seal_record: fixtures::record(8),
                previous_seal_crc32: 0x3c6d_d54f,
            }),
        ),
        raw(10, 1, 3),
        timer(11, 1),
        frame(
            12,
            1,
            Record::SegmentSeal(SegmentSeal {
                prefix_frame_count: 3,
                prefix_physical_len: 286,
                prefix_crc32: 2_529_098_518,
                prior_record: fixtures::record(11),
                has_gap: false,
                is_final: true,
            }),
        ),
        frame(
            13,
            1,
            Record::ArchiveSeal(ArchiveSeal {
                expected_segment_count: 2,
                prior_frame_count: 12,
                total_prefix_physical_bytes: 1382,
                prefix_crc32: 132_306_964,
                prior_record: fixtures::record(12),
                input_quality: InputQuality::GapsRecorded,
            }),
        ),
    ];
    (first, second)
}

pub fn committed_updates() -> Vec<MarketPayload> {
    vec![
        MarketPayload::Update(vec![
            LevelChange::Set(fixtures::level(Side::Bid, 2000, 2)),
            LevelChange::Delete {
                side: Side::Ask,
                price: domain::numeric::PriceTicks::new(2002).unwrap(),
            },
        ]),
        MarketPayload::Update(vec![LevelChange::Set(fixtures::level(Side::Ask, 2003, 1))]),
    ]
}
