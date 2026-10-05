use super::*;

impl HealthModel {
    pub(super) fn raw(
        &mut self,
        at: RecordNo,
        raw: &RawInput,
        env: &mut ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let mut state = self
            .streams
            .remove(&raw.stream)
            .ok_or(ModelError::UnknownDefinition)?;
        state.loss.raw(raw.attempt, raw.tag)?;
        if raw.tag != state.binding.tag {
            self.diagnostic(at, raw.stream, DiagnosticCode::ObsoleteScope, out);
            self.streams.insert(raw.stream, state);
            return Ok(());
        }
        if at.get() <= state.barrier {
            self.diagnostic(at, raw.stream, DiagnosticCode::PreBarrier, out);
            self.streams.insert(raw.stream, state);
            return Ok(());
        }
        let id = RawFrameId {
            archive: self.start.archive,
            record: at,
        };
        let fact = env
            .normalizations
            .get(&id)
            .cloned()
            .ok_or(ArtifactError::ArtifactUnverified)?;
        let kind = match fact.frame.validate() {
            Ok(kind) => kind,
            Err(error) => {
                let fault = if error == EventError::MixedFrameUnsupported {
                    Fault::MixedFrameUnsupported
                } else {
                    Fault::Structural(error)
                };
                self.fault(&mut state, at, fault, out);
                self.streams.insert(raw.stream, state);
                return Ok(());
            }
        };
        let policy = self.policy()?;
        let count =
            u32::try_from(fact.frame.outputs.len()).map_err(|_| EventError::EventTooLarge)?;
        if kind == FrameKind::Trades {
            env.prefix.insert_frame(fact.frame.clone());
            self.emit(&mut state, at, &fact.frame, None, env, out)?;
            state.last_applied_raw = Some(at);
            state.last_valid_sample_ns = Some(
                state
                    .last_valid_sample_ns
                    .map_or(raw.context.monotonic_ns.get(), |previous| {
                        previous.max(raw.context.monotonic_ns.get())
                    }),
            );
            state.quiet = None;
            self.refresh_freshness(&mut state, policy, at, out);
        } else if kind != FrameKind::NoMarketData {
            let frames = state.pending.len() as u64 + 1;
            let bytes = state
                .pending
                .iter()
                .map(|p| u64::from(p.raw_bytes))
                .sum::<u64>()
                + u64::from(fact.frame.raw_byte_len);
            let outputs = state
                .pending
                .iter()
                .map(|p| u64::from(p.outputs))
                .sum::<u64>()
                + u64::from(count);
            let exceeded = if frames > u64::from(policy.pending_max_frames) {
                Some("frames")
            } else if bytes > policy.pending_max_raw_bytes {
                Some("raw_bytes")
            } else if outputs > u64::from(policy.pending_max_outputs) {
                Some("outputs")
            } else {
                None
            };
            if let Some(field) = exceeded {
                self.fault(&mut state, at, Fault::PendingOverflow(field), out);
            } else if let Some(deadline_ns) = raw
                .context
                .monotonic_ns
                .get()
                .checked_add(policy.pending_wait_ns)
            {
                if self.evaluation_ns >= deadline_ns {
                    self.fault(&mut state, at, Fault::PendingTimeout, out);
                } else {
                    state.pending.push(PendingFrame {
                        raw: id,
                        raw_bytes: fact.frame.raw_byte_len,
                        outputs: count,
                        deadline_ns,
                        output_sha256: fact.output_sha256,
                        post_barrier_membership: fact.post_barrier_membership,
                        proof: None,
                    });
                }
            } else {
                self.fault(&mut state, at, Fault::PendingDeadlineOverflow, out);
            }
        }
        self.streams.insert(raw.stream, state);
        Ok(())
    }

    pub(super) fn verification(
        &mut self,
        at: RecordNo,
        wire: &VerificationEvidence,
        env: &mut ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let mut state = self
            .streams
            .remove(&wire.stream)
            .ok_or(ModelError::UnknownDefinition)?;
        let id = RawFrameId {
            archive: self.start.archive,
            record: wire.raw,
        };
        let recorded_raw = env
            .raw_records
            .get(&id)
            .ok_or(EventError::MissingRawInput)?;
        if wire.raw.get() <= state.barrier {
            self.diagnostic(at, wire.stream, DiagnosticCode::PreBarrier, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let context = self.context()?;
        let old_body = env.verifications.get(&wire.proof).is_some_and(|body| {
            body.scope.context != context || body.scope.tag != state.binding.tag
        });
        if wire.tag != state.binding.tag
            || recorded_raw.tag != state.binding.tag
            || recorded_raw.context.context != InputContext::Active(context)
            || old_body
        {
            self.diagnostic(at, wire.stream, DiagnosticCode::ObsoleteScope, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let config_ref = self.config.as_ref().ok_or(ModelError::UnknownDefinition)?.0;
        let body = env.verification(
            wire.proof,
            config_ref,
            state.profile_ref,
            self.start.archive,
        )?;
        let mut required = vec![wire.raw];
        if state.barrier > 0 {
            required.push(RecordNo::new(state.barrier)?);
        }
        env.check_basis(self.start.archive, at, &body.basis_records, &required)?;
        let source = env
            .prefix
            .raw(id)
            .cloned()
            .ok_or(EventError::MissingRawInput)?;
        let source_kind = source.validate()?;
        let expected_kind = match source_kind {
            FrameKind::Snapshot => Some(BookEvidenceKind::Snapshot),
            FrameKind::Delta => Some(BookEvidenceKind::Delta),
            _ => None,
        };
        let basic_match = body.scope == self.scope(&state)?
            && body.raw == wire.raw
            && body.kind == wire.kind
            && Some(wire.kind) == expected_kind
            && wire.profile == state.binding.feed_profile
            && source.source.binding == state.binding
            && body.raw_sample_ns == source.source.received.monotonic.ns.get()
            && body.not_before_ns == state.barrier_evaluation_ns
            && body.raw_sample_ns >= body.not_before_ns
            && body
                .valid_until_ns
                .is_none_or(|until| until > body.not_before_ns)
            && usize::try_from(body.output_count).ok() == Some(source.outputs.len());
        if !basic_match {
            self.fault(&mut state, at, Fault::ProofConflict, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        if state
            .last_applied_raw
            .is_some_and(|frontier| wire.raw <= frontier)
        {
            let first = env
                .first_proofs
                .get(&id)
                .ok_or(ArtifactError::MissingArtifact)?;
            if first.body == body {
                self.diagnostic(at, wire.stream, DiagnosticCode::AlreadyApplied, out);
            } else {
                self.fault(&mut state, at, Fault::ProofConflict, out);
            }
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let Some(position) = state.pending.iter().position(|pending| pending.raw == id) else {
            self.fault(&mut state, at, Fault::ProofConflict, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        };
        let pending = &state.pending[position];
        if pending.output_sha256 != body.output_sha256 || !pending.post_barrier_membership {
            self.fault(&mut state, at, Fault::ProofConflict, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        if let Some(first) = &pending.proof {
            if first.body == body {
                self.diagnostic(at, wire.stream, DiagnosticCode::AlreadyVerified, out);
            } else {
                self.fault(&mut state, at, Fault::ProofConflict, out);
            }
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        if body
            .valid_until_ns
            .is_some_and(|until| self.evaluation_ns >= until)
        {
            self.fault(&mut state, at, Fault::ProofExpired, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let first = AcceptedProof {
            record: at,
            reference: wire.proof,
            body,
        };
        state.pending[position].proof = Some(first.clone());
        env.first_proofs.insert(id, first);
        self.release(&mut state, at, env, out)?;
        self.streams.insert(wire.stream, state);
        Ok(())
    }

    fn release(
        &self,
        state: &mut StreamState,
        at: RecordNo,
        env: &mut ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let policy = self.policy()?;
        while state
            .pending
            .first()
            .is_some_and(|pending| pending.proof.is_some())
        {
            let pending = state.pending.remove(0);
            let proof = pending.proof.ok_or(ArtifactError::MissingArtifact)?;
            if proof
                .body
                .valid_until_ns
                .is_some_and(|until| self.evaluation_ns >= until)
            {
                self.fault(state, at, Fault::ProofExpired, out);
                break;
            }
            let source = env
                .prefix
                .raw(pending.raw)
                .cloned()
                .ok_or(EventError::MissingRawInput)?;
            let kind = source.validate()?;
            if kind == FrameKind::Snapshot {
                if policy.fields.require_two_sided_snapshot
                    && !source.outputs[0].payload.two_sided_snapshot()
                {
                    self.fault(state, at, Fault::SnapshotNotTwoSided, out);
                    break;
                }
            } else if kind == FrameKind::Delta {
                if state.anchor.is_none()
                    || !matches!(
                        state.book,
                        Some(BookValidity::Warming | BookValidity::Usable)
                    )
                {
                    self.fault(state, at, Fault::NeedsSnapshot, out);
                    break;
                }
            } else {
                self.fault(state, at, Fault::MixedFrameUnsupported, out);
                break;
            }
            // All outputs of this source frame are validated before any state
            // or output of that frame is committed. Earlier complete frames in
            // this release step remain auditable if a later frame fails.
            self.emit(state, at, &source, Some(proof.reference), env, out)?;
            if kind == FrameKind::Snapshot {
                state.book = Some(BookValidity::Warming);
                state.anchor = Some(Anchor {
                    raw: source.source.raw,
                    original_sample_ns: source.source.received.monotonic.ns.get(),
                });
                state.progress = 0;
                state.witness = None;
            } else if state.book == Some(BookValidity::Warming) {
                let threshold = policy.fields.warmup_min_updates.unwrap_or(0);
                for _ in &source.outputs {
                    if state.progress < threshold {
                        state.progress = state
                            .progress
                            .checked_add(1)
                            .ok_or(EventError::EventTooLarge)?;
                    }
                }
            }
            let sample = source.source.received.monotonic.ns.get();
            state.last_valid_sample_ns = Some(
                state
                    .last_valid_sample_ns
                    .map_or(sample, |old| old.max(sample)),
            );
            state.last_applied_raw = Some(source.source.raw.record);
            state.quiet = None;
            self.refresh_freshness(state, policy, at, out);
        }
        Ok(())
    }

    fn emit(
        &self,
        state: &mut StreamState,
        at: RecordNo,
        source: &NormalizedFrame,
        proof: Option<ArtifactRef>,
        env: &ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let mut references = vec![
            self.config.as_ref().ok_or(ModelError::UnknownDefinition)?.0,
            state.profile_ref,
        ];
        references.extend(proof);
        references.sort_unstable();
        references.dedup();
        let mut frame_effects = Vec::new();
        for (source_index, normalized) in source.outputs.iter().enumerate() {
            let output_index = u32::try_from(out.effects.len() + frame_effects.len())
                .map_err(|_| EventError::EventTooLarge)?;
            let cursor = EventCursor {
                apply_record: at,
                output_index: OutputIndex::new(output_index),
            };
            let event = EventEnvelope {
                schema_version: 1,
                event_id: EventId {
                    archive: self.start.archive,
                    cursor,
                    normalizer: source.source.context.normalizer,
                },
                source_candidate: SourceCandidateKey {
                    raw: source.source.raw,
                    index: RawSubIndex::new(
                        u32::try_from(source_index).map_err(|_| EventError::EventTooLarge)?,
                    ),
                    normalizer: source.source.context.normalizer,
                },
                raw_input_ref: source.source.raw,
                source_ingest_order: source.source.raw.record,
                binding: source.source.binding.clone(),
                source_timestamp: normalized.timestamp.clone(),
                received: source.source.received,
                applied_at: MonotonicSample {
                    scope: self.clock(),
                    ns: MonotonicNs::new(self.evaluation_ns),
                },
                context: source.source.context,
                available_at: cursor,
                as_of: CausalBasis {
                    record_frontier: at,
                    prior_effects: vec![],
                },
                record_schema_version: 1,
                artifact_refs: references.clone(),
                payload: normalized.payload.clone(),
            };
            let prior: Vec<_> = out.effects.iter().map(|e| e.event_id.cursor).collect();
            let view = ApplyingView {
                history: &env.prefix,
                current: self.at(at),
                source,
            };
            event.validate(
                &EnvelopeContext {
                    apply: self.at(at),
                    binding: &state.binding,
                    active: self.context()?,
                    clock: self.clock(),
                    artifact_refs: &references,
                    prior_outputs_this_step: &prior,
                },
                &view,
            )?;
            frame_effects.push(event);
        }
        state.last_batch_effects = frame_effects.iter().map(|event| event.event_id).collect();
        if let Some(last) = frame_effects.last() {
            state.last_event_cursor = Some(last.event_id.cursor);
        }
        out.effects.extend(frame_effects);
        Ok(())
    }
}

struct ApplyingView<'a> {
    history: &'a super::super::fixtures::MemoryPrefix,
    current: RecordRef,
    source: &'a NormalizedFrame,
}

impl PrefixView for ApplyingView<'_> {
    fn raw(&self, id: RawFrameId) -> Option<&NormalizedFrame> {
        if id == self.source.source.raw {
            Some(self.source)
        } else {
            self.history.raw(id)
        }
    }

    fn record_exists(&self, record: RecordRef) -> bool {
        record == self.current || self.history.record_exists(record)
    }

    fn effect_exists(&self, event: EventRef) -> bool {
        self.history.effect_exists(event)
    }
}
