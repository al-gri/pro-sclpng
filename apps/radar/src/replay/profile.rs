//! Pinned synthetic diagnostic inputs, not exchange units or verified artifacts.

use domain::artifact::ArtifactRef;
use domain::event::{ActiveContext, ClockScope, InputContext};
use domain::identity::*;
use domain::numeric::ExactDecimal;
use domain::policy::{DurabilityMode, HealthPolicy, PolicyFields, RecordingGate, SilenceRule};
use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
use domain::record::{
    ConfigDefinition, InstrumentSpecRecord, ProvenanceKind, StreamDefinition, WireContext,
};
use market_data::DecodeLimits;

pub const NAME: &str = "synthetic-rec001f1-v1";
pub const MAX_ARCHIVE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_RECORDS: usize = 256;
pub const MAX_STREAMS: usize = 2;
pub const PENDING_WAIT_NS: u64 = 100;
pub const SYNTHETIC_UNIX_BASE_NS: i64 = 1_770_000_000_000_000_000;

pub fn decode_limits() -> DecodeLimits {
    DecodeLimits {
        max_message_bytes: 4096,
        max_trades_per_message: 16,
        max_container_items: 256,
        max_nesting_depth: 16,
        max_string_bytes: 128,
    }
}

pub fn health_policy() -> HealthPolicy {
    HealthPolicy {
        fields: PolicyFields {
            silence_rule: SilenceRule::UnknownOnSilence,
            freshness_deadline_ns: Some(50),
            warmup_min_updates: Some(1),
            warmup_min_elapsed_ns: Some(0),
            allow_quiet_with_proof: false,
            require_two_sided_snapshot: true,
            recording_gate: RecordingGate::Written,
        },
        pending_max_frames: 3,
        pending_max_raw_bytes: 16 * 1024,
        pending_max_outputs: 3,
        pending_wait_ns: PENDING_WAIT_NS,
        quiet_max_lifetime_ns: None,
    }
}

pub fn archive_id() -> ArchiveId {
    ArchiveId::new([0xf1; 16]).expect("fixed nonzero synthetic archive")
}

pub fn session_id() -> CaptureSessionId {
    CaptureSessionId::new([0xc1; 16]).expect("fixed nonzero synthetic session")
}

pub fn clock_id() -> ClockId {
    ClockId::new(1).expect("fixed nonzero synthetic clock")
}

pub fn clock_scope() -> ClockScope {
    ClockScope {
        session: session_id(),
        clock: clock_id(),
    }
}

pub fn active_context() -> ActiveContext {
    ActiveContext {
        config: ConfigVersion::new(1).expect("fixed synthetic config"),
        normalizer: NormalizerVersion::new(1).expect("fixed synthetic normalizer"),
    }
}

/// A grammar-valid unresolved reference; no body or applicability is asserted.
pub fn artifact() -> ArtifactRef {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("fixed artifact reference grammar")
}

pub fn instrument() -> InstrumentRef {
    InstrumentRef {
        venue: Token::new("Bitget").expect("fixed venue token"),
        market: MarketKind::Perpetual,
        product_namespace: Token::new("usdt-futures").expect("fixed category token"),
        native_symbol: Token::new("BTCUSDT").expect("fixed symbol token"),
    }
}

pub fn spec_ref() -> SpecRef {
    SpecRef {
        instrument: instrument(),
        version: SpecVersion::new(1).expect("fixed synthetic spec version"),
    }
}

pub fn stream_binding() -> StreamBinding {
    StreamBinding {
        id: StreamId::new(1).expect("fixed stream"),
        instrument_slot: InstrumentSlot::new(1).expect("fixed slot"),
        spec: spec_ref(),
        connection_id: ConnectionId::new(1).expect("fixed connection"),
        channel: Channel::BookNormal,
        book_id: Some(BookId::new(1).expect("fixed book")),
        tag: EpochTag {
            spec: SpecVersion::new(1).expect("fixed spec epoch"),
            connection: ConnectionEpoch::new(1).expect("fixed connection epoch"),
            subscription: SubscriptionEpoch::new(1).expect("fixed subscription epoch"),
            book: Some(BookEpoch::new(1).expect("fixed book epoch")),
        },
        feed_profile: FeedProfileVersion::new(1).expect("fixed synthetic feed profile"),
    }
}

pub fn bootstrap_context(sample_ns: u64) -> WireContext {
    WireContext {
        unix_ns: LocalUnixNs::new(
            SYNTHETIC_UNIX_BASE_NS + i64::try_from(sample_ns).expect("fixed small sample"),
        ),
        monotonic_ns: MonotonicNs::new(sample_ns),
        context: InputContext::Bootstrap,
    }
}

pub fn instrument_spec() -> InstrumentSpecRecord {
    InstrumentSpecRecord {
        context: bootstrap_context(10),
        slot: InstrumentSlot::new(1).expect("fixed slot"),
        numeric: NumericSpec::new(NumericSpecFields {
            reference: spec_ref(),
            price_units: PriceUnits {
                quote: Token::new("SYNTHETIC_QUOTE").expect("fixed synthetic quote"),
                basis: Token::new("SYNTHETIC_BASE").expect("fixed synthetic basis"),
            },
            quantity_unit: Token::new("SYNTHETIC_UNKNOWN")
                .expect("unresolved quantity placeholder"),
            base_asset: Token::new("SYNTHETIC_BASE").expect("fixed synthetic base"),
            price_increment: ExactDecimal::ONE,
            quantity_increment: ExactDecimal::ONE,
            quantity_to_base_multiplier: None,
        })
        .expect("valid synthetic numeric descriptor"),
        provenance: artifact(),
    }
}

/// Optional pinned diagnostic descriptor for testing an unsupported activation.
/// Defining version 2 does not activate it; the ordinary fixture omits it.
pub fn inactive_instrument_spec() -> InstrumentSpecRecord {
    let mut inactive = instrument_spec();
    inactive.context = bootstrap_context(15);
    let mut fields = inactive.numeric.fields().clone();
    fields.reference.version = SpecVersion::new(2).expect("fixed inactive synthetic spec version");
    inactive.numeric =
        NumericSpec::new(fields).expect("valid inactive synthetic numeric descriptor");
    inactive
}

pub fn stream_definition() -> StreamDefinition {
    StreamDefinition {
        context: bootstrap_context(20),
        binding: stream_binding(),
        provenance: artifact(),
    }
}

pub fn config_definition() -> ConfigDefinition {
    ConfigDefinition {
        context: bootstrap_context(30),
        next: active_context(),
        provenance_kind: ProvenanceKind::Synthetic,
        evidence: artifact(),
        fields: health_policy().fields,
    }
}

pub const fn durability_mode() -> DurabilityMode {
    DurabilityMode::Buffered
}
