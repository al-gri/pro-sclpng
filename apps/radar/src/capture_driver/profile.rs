//! Separate F-2 synthetic, unverified profile. No exchange metadata is asserted.
use domain::artifact::ArtifactRef;
use domain::capture_session::RetentionBudget;
use domain::event::{ActiveContext, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, PolicyFields, RecordingGate, SilenceRule};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::*;
use market_data::ReceiveStamp;

use super::DriverError;

pub const SYNTHETIC_UNIX_BASE_NS: i64 = 1_800_000_000_000_000_000;
pub const CONNECTED_NS: u64 = 100;
pub const ACK: &[u8] = br#"{"event":"subscribe","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"connId":"synthetic-f2"}"#;
pub const BOOK_ONE: &[u8] = br#"{"action":"snapshot","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["101","3"]],"b":[["100","2"]],"pseq":0,"seq":10,"ts":"2000"}],"ts":2000}"#;
pub const BOOK_TWO: &[u8] = br#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["102","4"]],"b":[],"pseq":10,"seq":11,"ts":"1000"}],"ts":1000}"#;
pub const BOOK_THREE: &[u8] = br#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["102","5"]],"b":[],"pseq":11,"seq":12,"ts":"1500"}],"ts":1500}"#;

fn id<T>(value: Result<T, IdentityError>) -> T {
    value.expect("fixed shape-valid synthetic F-2 identity")
}

pub fn active() -> ActiveContext {
    ActiveContext {
        config: id(ConfigVersion::new(2)),
        normalizer: id(NormalizerVersion::new(2)),
    }
}

pub fn binding() -> StreamBinding {
    let spec = id(SpecVersion::new(2));
    StreamBinding {
        id: id(StreamId::new(1)),
        instrument_slot: id(InstrumentSlot::new(1)),
        spec: SpecRef {
            instrument: InstrumentRef {
                venue: id(Token::new("bitget")),
                market: MarketKind::Perpetual,
                product_namespace: id(Token::new("usdt-futures")),
                native_symbol: id(Token::new("BTCUSDT")),
            },
            version: spec,
        },
        connection_id: id(ConnectionId::new(1)),
        channel: Channel::BookNormal,
        book_id: Some(id(BookId::new(1))),
        tag: EpochTag {
            spec,
            connection: id(ConnectionEpoch::new(1)),
            subscription: id(SubscriptionEpoch::new(1)),
            book: Some(id(BookEpoch::new(1))),
        },
        feed_profile: id(FeedProfileVersion::new(2)),
    }
}

pub fn budget() -> RetentionBudget {
    RetentionBudget {
        item_cap: 16,
        raw_frame_limit: 8,
        raw_byte_limit: 16_384,
        max_message_bytes: 4096,
    }
}

pub fn stamp(monotonic_ns: u64) -> Result<ReceiveStamp, DriverError> {
    let elapsed = i64::try_from(monotonic_ns).map_err(|_| DriverError::new("stamp_overflow"))?;
    let unix_ns = SYNTHETIC_UNIX_BASE_NS
        .checked_add(elapsed)
        .ok_or_else(|| DriverError::new("stamp_overflow"))?;
    Ok(ReceiveStamp {
        unix_ns,
        monotonic_ns,
    })
}

fn wire(number: u64) -> WireContext {
    let receive = stamp(number).expect("small fixed synthetic bootstrap stamp");
    WireContext {
        unix_ns: LocalUnixNs::new(receive.unix_ns),
        monotonic_ns: MonotonicNs::new(number),
        context: InputContext::Bootstrap,
    }
}

fn frame(number: u64, value: Record) -> RecordFrame {
    RecordFrame {
        record_no: id(RecordNo::new(number)),
        segment_no: SegmentNo::new(0),
        value,
    }
}

pub fn start_frame() -> RecordFrame {
    frame(
        1,
        Record::ArchiveStart(ArchiveStart {
            archive: id(ArchiveId::new([0xf2; 16])),
            session: id(CaptureSessionId::new([0xc2; 16])),
            clock: id(ClockId::new(2002)),
            mode: DurabilityMode::SyncBeforePublish,
            previous_archive: None,
        }),
    )
}

pub fn bootstrap() -> Vec<RecordFrame> {
    // This fixed digest names synthetic provenance, not verified Bitget evidence.
    let provenance: ArtifactRef =
        "sha256:f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2"
            .parse()
            .expect("fixed synthetic artifact reference");
    let stream = binding();
    vec![
        frame(
            2,
            Record::InstrumentSpec(InstrumentSpecRecord {
                context: wire(2),
                slot: stream.instrument_slot,
                numeric: NumericSpec::new(NumericSpecFields {
                    reference: stream.spec.clone(),
                    price_units: PriceUnits {
                        quote: id(Token::new("USDT")),
                        basis: id(Token::new("BASE")),
                    },
                    quantity_unit: id(Token::new("BTC")),
                    base_asset: id(Token::new("BTC")),
                    price_increment: ExactDecimal::ONE,
                    quantity_increment: ExactDecimal::ONE,
                    quantity_to_base_multiplier: Some(ExactDecimal::ONE),
                })
                .expect("fixed synthetic numeric spec"),
                provenance,
            }),
        ),
        frame(
            3,
            Record::StreamDefinition(StreamDefinition {
                context: wire(3),
                binding: stream,
                provenance,
            }),
        ),
        frame(
            4,
            Record::ConfigDefinition(ConfigDefinition {
                context: wire(4),
                next: active(),
                provenance_kind: ProvenanceKind::Synthetic,
                evidence: provenance,
                fields: PolicyFields {
                    silence_rule: SilenceRule::UnknownOnSilence,
                    freshness_deadline_ns: Some(1_000_000_000),
                    warmup_min_updates: Some(1),
                    warmup_min_elapsed_ns: Some(0),
                    allow_quiet_with_proof: false,
                    require_two_sided_snapshot: true,
                    recording_gate: RecordingGate::Durable,
                },
            }),
        ),
    ]
}
