//! origin=synthetic, schema=1, proposal=2. No real-feed values or live clock.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use domain::artifact::ArtifactRef;
use domain::event::*;
use domain::identity::*;
use domain::numeric::{PriceTicks, QuantitySteps};
use domain::policy::*;

pub fn archive() -> ArchiveId {
    ArchiveId::new([1; 16]).unwrap()
}

pub fn record(n: u64) -> RecordNo {
    RecordNo::new(n).unwrap()
}

pub fn record_ref(n: u64) -> RecordRef {
    RecordRef {
        archive: archive(),
        record: record(n),
    }
}

pub fn raw_id(n: u64) -> RawFrameId {
    RawFrameId {
        archive: archive(),
        record: record(n),
    }
}

pub fn cursor(n: u64, index: u32) -> EventCursor {
    EventCursor {
        apply_record: record(n),
        output_index: OutputIndex::new(index),
    }
}

pub fn clock() -> ClockScope {
    ClockScope {
        session: CaptureSessionId::new([2; 16]).unwrap(),
        clock: ClockId::new(1).unwrap(),
    }
}

pub fn active(config: u32, normalizer: u32) -> ActiveContext {
    ActiveContext {
        config: ConfigVersion::new(config).unwrap(),
        normalizer: NormalizerVersion::new(normalizer).unwrap(),
    }
}

pub fn artifact_ref(byte: u8) -> ArtifactRef {
    let hex = format!("{byte:02x}");
    format!("sha256:{}", hex.repeat(32)).parse().unwrap()
}

pub fn binding(id: u32, channel: Channel) -> StreamBinding {
    let is_book = channel != Channel::Trades;
    StreamBinding {
        id: StreamId::new(id).unwrap(),
        instrument_slot: InstrumentSlot::new(1).unwrap(),
        spec: SpecRef {
            instrument: InstrumentRef {
                venue: Token::new("SYN").unwrap(),
                market: MarketKind::Spot,
                product_namespace: Token::new("spot").unwrap(),
                native_symbol: Token::new("ABCUSD").unwrap(),
            },
            version: SpecVersion::new(1).unwrap(),
        },
        connection_id: ConnectionId::new(1).unwrap(),
        channel,
        book_id: is_book.then(|| BookId::new(id).unwrap()),
        tag: EpochTag {
            spec: SpecVersion::new(1).unwrap(),
            connection: ConnectionEpoch::new(1).unwrap(),
            subscription: SubscriptionEpoch::new(1).unwrap(),
            book: is_book.then(|| BookEpoch::new(1).unwrap()),
        },
        feed_profile: FeedProfileVersion::new(1).unwrap(),
    }
}

pub fn level(side: Side, price: u64, quantity: u64) -> Level {
    Level {
        side,
        price: PriceTicks::new(price).unwrap(),
        quantity: QuantitySteps::new(quantity),
    }
}

pub fn snapshot() -> MarketPayload {
    MarketPayload::Snapshot(vec![level(Side::Bid, 2000, 1), level(Side::Ask, 2002, 1)])
}

pub fn update() -> MarketPayload {
    MarketPayload::Update(vec![LevelChange::Set(level(Side::Bid, 2000, 2))])
}

pub fn trade() -> MarketPayload {
    MarketPayload::Trade(Trade {
        price: PriceTicks::new(2001).unwrap(),
        quantity: QuantitySteps::new(1),
        aggressor: Aggressor::Unknown,
        rpi: RpiAttribute::Unknown,
        source_trade_id: None,
        book_link: TradeBookLink::Unknown,
    })
}

pub fn frame(n: u64, payloads: Vec<MarketPayload>, stream: StreamBinding) -> NormalizedFrame {
    NormalizedFrame {
        source: SourceMetadata {
            raw: raw_id(n),
            binding: stream,
            context: active(1, 1),
            received: ReceiveSample {
                unix_ns: LocalUnixNs::new(i64::try_from(n).unwrap()),
                monotonic: MonotonicSample {
                    scope: clock(),
                    ns: MonotonicNs::new(n),
                },
            },
        },
        raw_byte_len: 16,
        outputs: payloads
            .into_iter()
            .map(|payload| NormalizedOutput {
                payload,
                timestamp: SourceTimestamp::Unknown,
            })
            .collect(),
    }
}

pub fn envelope(
    frame: &NormalizedFrame,
    source_index: u32,
    apply: u64,
    index: u32,
) -> EventEnvelope {
    let output = &frame.outputs[usize::try_from(source_index).unwrap()];
    EventEnvelope {
        schema_version: 1,
        event_id: EventId {
            archive: frame.source.raw.archive,
            cursor: cursor(apply, index),
            normalizer: frame.source.context.normalizer,
        },
        source_candidate: SourceCandidateKey {
            raw: frame.source.raw,
            index: RawSubIndex::new(source_index),
            normalizer: frame.source.context.normalizer,
        },
        raw_input_ref: frame.source.raw,
        source_ingest_order: frame.source.raw.record,
        binding: frame.source.binding.clone(),
        source_timestamp: output.timestamp.clone(),
        received: frame.source.received,
        applied_at: MonotonicSample {
            scope: clock(),
            ns: MonotonicNs::new(apply),
        },
        context: frame.source.context,
        available_at: cursor(apply, index),
        as_of: CausalBasis {
            record_frontier: record(apply),
            prior_effects: vec![],
        },
        record_schema_version: 1,
        artifact_refs: vec![],
        payload: output.payload.clone(),
    }
}

pub fn policy() -> HealthPolicy {
    HealthPolicy {
        fields: PolicyFields {
            silence_rule: SilenceRule::UnknownOnSilence,
            freshness_deadline_ns: Some(100),
            warmup_min_updates: Some(1),
            warmup_min_elapsed_ns: Some(0),
            allow_quiet_with_proof: false,
            require_two_sided_snapshot: true,
            recording_gate: RecordingGate::Durable,
        },
        pending_max_frames: 2,
        pending_max_raw_bytes: 256,
        pending_max_outputs: 4,
        pending_wait_ns: 50,
        quiet_max_lifetime_ns: None,
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryPrefix {
    pub frames: BTreeMap<RawFrameId, NormalizedFrame>,
    pub records: BTreeSet<RecordRef>,
    pub effects: HashSet<EventRef>,
    pub configs: BTreeSet<ConfigVersion>,
    pub normalizers: BTreeMap<NormalizerVersion, ArtifactRef>,
}

impl MemoryPrefix {
    pub fn through(n: u64) -> Self {
        Self {
            records: (1..=n).map(record_ref).collect(),
            ..Self::default()
        }
    }

    pub fn insert_frame(&mut self, frame: NormalizedFrame) {
        self.records.insert(RecordRef {
            archive: frame.source.raw.archive,
            record: frame.source.raw.record,
        });
        self.frames.insert(frame.source.raw, frame);
    }
}

impl PrefixView for MemoryPrefix {
    fn raw(&self, id: RawFrameId) -> Option<&NormalizedFrame> {
        self.frames.get(&id)
    }

    fn record_exists(&self, record: RecordRef) -> bool {
        self.records.contains(&record)
    }

    fn effect_exists(&self, event: EventRef) -> bool {
        self.effects.contains(&event)
    }
}

impl DefinitionHistory for MemoryPrefix {
    fn config_defined(&self, config: ConfigVersion) -> bool {
        self.configs.contains(&config)
    }

    fn normalizer_reference(&self, normalizer: NormalizerVersion) -> Option<ArtifactRef> {
        self.normalizers.get(&normalizer).copied()
    }
}
