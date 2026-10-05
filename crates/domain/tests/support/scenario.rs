//! Deterministic synthetic fixtures for the recorded-input reference model.
//! Mock digest observations are deliberately supplied, never computed as proof.

use domain::artifact::*;
use domain::event::*;
use domain::identity::*;
use domain::policy::*;
use domain::qualified::*;
use domain::record::*;

use super::artifacts::*;
use super::bodies::*;
use super::fixtures;
use super::health::*;
use super::model_env::ModelEnv;

pub const NORMALIZER_REF: u8 = 1;
pub const CONFIG_REF: u8 = 2;
pub const PROFILE_REF: u8 = 3;
pub const INSTRUMENT_REF: u8 = 4;
pub const BASIS_REF: u8 = 5;

pub fn reference(index: u64) -> ArtifactRef {
    format!("sha256:{index:064x}").parse().unwrap()
}

pub fn mock_node(
    index: u64,
    kind: ArtifactKind,
    logical: &str,
    revision: u32,
    mut dependencies: Vec<ArtifactRef>,
) -> SyntheticArtifact {
    dependencies.sort_unstable();
    dependencies.dedup();
    let reference = reference(index);
    SyntheticArtifact {
        reference,
        metadata: ArtifactMetadata {
            identity: ArtifactIdentity {
                archive: fixtures::archive(),
                kind,
                logical: Token::new(logical).unwrap(),
                revision,
            },
            format_version: 1,
            body_schema: 1,
            body_length: 3,
            body_sha256: [42; 32],
            dependencies: dependencies.clone(),
        },
        descriptor_length: 100,
        observed: Some(SyntheticBytes {
            descriptor_digest: *reference.claimed_digest(),
            body_digest: [42; 32],
            body_length: 3,
        }),
        required_dependencies: dependencies,
        optional_basis: vec![],
        applicable: true,
    }
}

pub fn numeric_spec() -> NumericSpec {
    NumericSpec::new(NumericSpecFields {
        reference: fixtures::binding(1, Channel::BookNormal).spec,
        price_units: PriceUnits {
            quote: Token::new("USD").unwrap(),
            basis: Token::new("ABC").unwrap(),
        },
        quantity_unit: Token::new("contract").unwrap(),
        base_asset: Token::new("ABC").unwrap(),
        price_increment: "0.05".parse().unwrap(),
        quantity_increment: "1".parse().unwrap(),
        quantity_to_base_multiplier: Some("0.01".parse().unwrap()),
    })
    .unwrap()
}

#[derive(Clone, Debug)]
pub struct Scenario {
    pub model: HealthModel,
    pub env: ModelEnv,
    pub next_artifact: u64,
}

impl Scenario {
    pub fn initial(policy: HealthPolicy) -> Self {
        let mut env = ModelEnv::default();
        let normalizer_ref = reference(NORMALIZER_REF.into());
        let config_ref = reference(CONFIG_REF.into());
        let profile_ref = reference(PROFILE_REF.into());
        let instrument_ref = reference(INSTRUMENT_REF.into());
        let basis_ref = reference(BASIS_REF.into());
        let binding = fixtures::binding(1, Channel::BookNormal);
        let config = ConfigBody {
            proposal_revision: 2,
            next: fixtures::active(1, 1),
            normalizer_ref,
            provenance: ProvenanceKind::Synthetic,
            policy,
        };
        let profile = FeedProfileBody {
            stream: binding.id,
            instrument: binding.spec.instrument.clone(),
            channel: binding.channel,
            version: binding.feed_profile,
            supported_normalizers: vec![normalizer_ref],
            basis: basis_ref,
        };
        for node in [
            mock_node(1, ArtifactKind::Normalizer, "normalizer", 1, vec![]),
            mock_node(2, ArtifactKind::Config, "config", 1, vec![normalizer_ref]),
            mock_node(
                3,
                ArtifactKind::FeedProfile,
                "stream/1",
                1,
                vec![normalizer_ref, basis_ref],
            ),
            mock_node(4, ArtifactKind::InstrumentSpec, "instrument/1", 1, vec![]),
            mock_node(5, ArtifactKind::Basis, "synthetic-basis", 1, vec![]),
        ] {
            env.resolver.supplied.insert(node.reference, node);
        }
        env.configs.insert(config_ref, config.clone());
        env.profiles.insert(profile_ref, profile);
        let numeric = numeric_spec();
        env.instruments
            .insert(instrument_ref, (binding.instrument_slot, numeric.clone()));
        let model = HealthModel::new(
            ArchiveStart {
                archive: fixtures::archive(),
                session: fixtures::clock().session,
                clock: fixtures::clock().clock,
                mode: DurabilityMode::SyncBeforePublish,
                previous_archive: None,
            },
            &mut env,
        )
        .unwrap();
        let mut scenario = Self {
            model,
            env,
            next_artifact: 100,
        };
        scenario
            .submit(Record::InstrumentSpec(InstrumentSpecRecord {
                context: scenario.context(2),
                slot: binding.instrument_slot,
                numeric,
                provenance: instrument_ref,
            }))
            .unwrap();
        scenario
            .submit(Record::StreamDefinition(StreamDefinition {
                context: scenario.context(3),
                binding,
                provenance: profile_ref,
            }))
            .unwrap();
        scenario
            .submit(Record::ConfigDefinition(ConfigDefinition {
                context: scenario.context(4),
                next: config.next,
                provenance_kind: config.provenance,
                evidence: config_ref,
                fields: policy.fields,
            }))
            .unwrap();
        scenario.transport(Transport::Up, 5).unwrap();
        scenario
            .receipt(RecordingHealth::Healthy, WatermarkKind::Durable, Some(5), 6)
            .unwrap();
        scenario.timer(7).unwrap();
        scenario.timer(8).unwrap();
        scenario.gap(Reason::SourceGap, None, None, 9).unwrap();
        scenario
    }

    pub fn stream(&self) -> &StreamState {
        &self.model.streams[&StreamId::new(1).unwrap()]
    }

    pub fn context(&self, time: u64) -> WireContext {
        WireContext {
            unix_ns: LocalUnixNs::new(i64::try_from(time).unwrap_or(0)),
            monotonic_ns: MonotonicNs::new(time),
            context: self
                .model
                .timeline
                .active()
                .map_or(InputContext::Bootstrap, InputContext::Active),
        }
    }

    pub fn next_record(&self) -> RecordNo {
        self.model.last_record.checked_next().unwrap()
    }

    pub fn submit(&mut self, value: Record) -> Result<StepResult> {
        self.model.step(
            &RecordFrame {
                record_no: self.next_record(),
                segment_no: SegmentNo::new(0),
                value,
            },
            &mut self.env,
        )
    }

    pub fn timer(&mut self, time: u64) -> Result<StepResult> {
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Timer {
                stream: StreamId::new(1).unwrap(),
                timer_id: self.next_record().get(),
                deadline_ns: time,
            },
        }))
    }

    pub fn transport(&mut self, value: Transport, time: u64) -> Result<StepResult> {
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Transport {
                connection: self.stream().binding.connection_id,
                epoch: self.stream().binding.tag.connection,
                value,
            },
        }))
    }

    pub fn receipt(
        &mut self,
        health: RecordingHealth,
        kind: WatermarkKind,
        through: Option<u64>,
        time: u64,
    ) -> Result<StepResult> {
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Recording(RecordingEvidence {
                health,
                kind,
                through: through.map(|n| RecordNo::new(n).unwrap()),
                reason: if health == RecordingHealth::Healthy {
                    Reason::NoFault
                } else {
                    Reason::WriteFailure
                },
            }),
        }))
    }

    pub fn gap(
        &mut self,
        reason: Reason,
        range: Option<(u64, u64)>,
        count: Option<u64>,
        time: u64,
    ) -> Result<StepResult> {
        self.submit(Record::Gap(Gap {
            context: self.context(time),
            reason,
            scope: GapScope::ExplicitTargets(vec![GapTarget {
                stream: self.stream().binding.id,
                tag: self.stream().binding.tag,
                range: range.map(|(first, last)| {
                    (
                        CaptureAttemptNo::new(first).unwrap(),
                        CaptureAttemptNo::new(last).unwrap(),
                    )
                }),
                loss_count: count,
            }]),
        }))
    }

    pub fn raw_input(
        &mut self,
        outputs: Vec<MarketPayload>,
        time: u64,
        attempt: Option<u64>,
        raw_len: Option<usize>,
    ) -> RawInput {
        let n = self.next_record();
        let binding = self.stream().binding.clone();
        let raw_id = RawFrameId {
            archive: self.model.start.archive,
            record: n,
        };
        let context = self.context(time);
        let mut bytes = format!("synthetic:{}:{}", n.get(), outputs.len()).into_bytes();
        if let Some(length) = raw_len {
            bytes.resize(length, b'_');
        }
        let raw = RawInput {
            context,
            stream: binding.id,
            tag: binding.tag,
            attempt: CaptureAttemptNo::new(
                attempt.unwrap_or(self.stream().loss.accounted_frontier + 1),
            )
            .unwrap(),
            bytes,
        };
        let frame = NormalizedFrame {
            source: SourceMetadata {
                raw: raw_id,
                binding,
                context: self.model.context().unwrap(),
                received: ReceiveSample {
                    unix_ns: raw.context.unix_ns,
                    monotonic: MonotonicSample {
                        scope: self.model.clock(),
                        ns: MonotonicNs::new(time),
                    },
                },
            },
            raw_byte_len: u32::try_from(raw.bytes.len()).unwrap(),
            outputs: outputs
                .into_iter()
                .map(|payload| NormalizedOutput {
                    payload,
                    timestamp: SourceTimestamp::Unknown,
                })
                .collect(),
        };
        self.env.normalizations.insert(
            raw_id,
            SyntheticNormalization {
                frame,
                output_sha256: [u8::try_from(n.get() % 251).unwrap(); 32],
                post_barrier_membership: true,
            },
        );
        raw
    }

    pub fn raw(&mut self, outputs: Vec<MarketPayload>, time: u64) -> Result<StepResult> {
        let raw = self.raw_input(outputs, time, None, None);
        self.submit(Record::RawInput(raw))
    }

    pub fn proof_body(&self, raw_record: u64) -> VerificationBody {
        let id = RawFrameId {
            archive: self.model.start.archive,
            record: RecordNo::new(raw_record).unwrap(),
        };
        let fact = &self.env.normalizations[&id];
        VerificationBody {
            scope: self.model.scope(self.stream()).unwrap(),
            raw: id.record,
            kind: if matches!(fact.frame.outputs[0].payload, MarketPayload::Snapshot(_)) {
                BookEvidenceKind::Snapshot
            } else {
                BookEvidenceKind::Delta
            },
            raw_sample_ns: fact.frame.source.received.monotonic.ns.get(),
            not_before_ns: self.stream().barrier_evaluation_ns,
            valid_until_ns: None,
            output_count: u32::try_from(fact.frame.outputs.len()).unwrap(),
            output_sha256: fact.output_sha256,
            continuity_basis: reference(BASIS_REF.into()),
            basis_records: (1..self.next_record().get())
                .map(|n| RecordNo::new(n).unwrap())
                .collect(),
        }
    }

    pub fn put_verification(&mut self, body: VerificationBody) -> VerificationEvidence {
        let index = self.next_artifact;
        self.next_artifact += 1;
        let config_ref = self.model.config.as_ref().unwrap().0;
        let profile_ref = self.stream().profile_ref;
        let logical = format!("stream/{}/raw/{}", body.scope.stream.get(), body.raw.get());
        let revision = self
            .env
            .resolver
            .supplied
            .values()
            .filter(|node| {
                node.metadata.identity.kind == ArtifactKind::Verification
                    && node.metadata.identity.logical.as_str() == logical
            })
            .count()
            + 1;
        let node = mock_node(
            index,
            ArtifactKind::Verification,
            &logical,
            u32::try_from(revision).unwrap(),
            vec![config_ref, profile_ref, body.continuity_basis],
        );
        let wire = VerificationEvidence {
            stream: body.scope.stream,
            tag: body.scope.tag,
            raw: body.raw,
            kind: body.kind,
            profile: body.scope.profile,
            proof: node.reference,
        };
        self.env.verifications.insert(node.reference, body);
        self.env.resolver.supplied.insert(node.reference, node);
        wire
    }

    pub fn proof(&mut self, raw_record: u64, time: u64) -> Result<StepResult> {
        let body = self.proof_body(raw_record);
        self.proof_with(body, time)
    }

    pub fn proof_with(&mut self, body: VerificationBody, time: u64) -> Result<StepResult> {
        let wire = self.put_verification(body);
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Verification(wire),
        }))
    }

    pub fn warmup_body(&self, time: u64) -> WarmupBody {
        let anchor = self.stream().anchor.as_ref().unwrap();
        WarmupBody {
            scope: self.model.scope(self.stream()).unwrap(),
            anchor: anchor.raw.record,
            update_count: self.stream().progress,
            elapsed_ns: time - anchor.original_sample_ns,
            observed_at_ns: time,
            basis_records: (1..self.next_record().get())
                .map(|n| RecordNo::new(n).unwrap())
                .collect(),
        }
    }

    pub fn warmup_with(&mut self, body: WarmupBody, time: u64) -> Result<StepResult> {
        let logical = format!(
            "stream/{}/anchor/{}",
            body.scope.stream.get(),
            body.anchor.get()
        );
        let revision = self
            .env
            .resolver
            .supplied
            .values()
            .filter(|node| {
                node.metadata.identity.kind == ArtifactKind::Warmup
                    && node.metadata.identity.logical.as_str() == logical
            })
            .count()
            + 1;
        let node = mock_node(
            self.next_artifact,
            ArtifactKind::Warmup,
            &logical,
            u32::try_from(revision).unwrap(),
            vec![
                self.model.config.as_ref().unwrap().0,
                self.stream().profile_ref,
            ],
        );
        self.next_artifact += 1;
        let wire = WarmupEvidence {
            stream: body.scope.stream,
            tag: body.scope.tag,
            anchor: body.anchor,
            update_count: body.update_count,
            elapsed_ns: body.elapsed_ns,
            proof: node.reference,
        };
        self.env.warmups.insert(node.reference, body);
        self.env.resolver.supplied.insert(node.reference, node);
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Warmup(wire),
        }))
    }

    pub fn warmup(&mut self, time: u64) -> Result<StepResult> {
        self.warmup_with(self.warmup_body(time), time)
    }

    pub fn recovered() -> Self {
        let mut s = Self::initial(fixtures::policy());
        s.raw(vec![fixtures::snapshot()], 10).unwrap();
        s.proof(10, 11).unwrap();
        s.raw(vec![fixtures::update()], 12).unwrap();
        s.proof(12, 13).unwrap();
        s.warmup(14).unwrap();
        s
    }

    pub fn config_change(
        &mut self,
        version: u32,
        normalizer: u32,
        policy: HealthPolicy,
        time: u64,
    ) -> Result<StepResult> {
        let old_normalizer = self.model.config.as_ref().unwrap().1.normalizer_ref;
        let normalizer_ref = if self.model.context().unwrap().normalizer.get() == normalizer {
            old_normalizer
        } else {
            let node = mock_node(
                self.next_artifact,
                ArtifactKind::Normalizer,
                "normalizer",
                normalizer,
                vec![],
            );
            self.next_artifact += 1;
            let reference = node.reference;
            self.env.resolver.supplied.insert(reference, node);
            reference
        };
        let node = mock_node(
            self.next_artifact,
            ArtifactKind::Config,
            "config",
            version,
            vec![normalizer_ref],
        );
        self.next_artifact += 1;
        let body = ConfigBody {
            proposal_revision: 2,
            next: fixtures::active(version, normalizer),
            normalizer_ref,
            provenance: ProvenanceKind::Synthetic,
            policy,
        };
        let definition = ConfigDefinition {
            context: self.context(time),
            next: body.next,
            provenance_kind: body.provenance,
            evidence: node.reference,
            fields: body.policy.fields,
        };
        self.env.configs.insert(node.reference, body);
        self.env.resolver.supplied.insert(node.reference, node);
        self.submit(Record::ConfigDefinition(definition))
    }

    pub fn epoch_connection(&mut self, time: u64) -> Result<StepResult> {
        let binding = &self.stream().binding;
        let change = EpochChange::Connection {
            owner: binding.connection_id,
            expected: binding.tag.connection,
            next: binding.tag.connection.checked_next().unwrap(),
        };
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::EpochAdvance {
                change,
                reason: Reason::Reconnect,
            },
        }))
    }

    pub fn freshness_body(
        &self,
        freshness: Freshness,
        observed: u64,
        from: Option<u64>,
        until: Option<u64>,
    ) -> FreshnessBody {
        FreshnessBody {
            scope: self.model.scope(self.stream()).unwrap(),
            anchor: self.stream().anchor.as_ref().map(|a| a.raw.record),
            basis: self.stream().last_applied_raw.unwrap(),
            freshness,
            observed_at_ns: observed,
            valid_from_ns: from,
            valid_until_ns: until,
            basis_records: (1..self.next_record().get())
                .map(|n| RecordNo::new(n).unwrap())
                .collect(),
        }
    }

    pub fn freshness_with(&mut self, body: FreshnessBody, time: u64) -> Result<StepResult> {
        let logical = format!(
            "stream/{}/basis/{}",
            body.scope.stream.get(),
            body.basis.get()
        );
        let revision = self
            .env
            .resolver
            .supplied
            .values()
            .filter(|node| {
                node.metadata.identity.kind == ArtifactKind::Freshness
                    && node.metadata.identity.logical.as_str() == logical
            })
            .count()
            + 1;
        let node = mock_node(
            self.next_artifact,
            ArtifactKind::Freshness,
            &logical,
            u32::try_from(revision).unwrap(),
            vec![
                self.model.config.as_ref().unwrap().0,
                self.stream().profile_ref,
            ],
        );
        self.next_artifact += 1;
        let wire = FreshnessEvidence {
            stream: body.scope.stream,
            tag: body.scope.tag,
            freshness: body.freshness,
            basis: Some(body.basis),
            proof: node.reference,
        };
        self.env.freshness.insert(node.reference, body);
        self.env.resolver.supplied.insert(node.reference, node);
        self.submit(Record::Control(ControlRecord {
            context: self.context(time),
            value: Control::Freshness(wire),
        }))
    }
}
