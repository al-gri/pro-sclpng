use super::*;

impl HealthModel {
    pub(super) fn preflight(&self, input: &RecordFrame, env: &mut ModelEnv) -> Result<()> {
        if let Some((reference, expected)) = &self.config {
            let actual = env.config(*reference, self.start.archive)?;
            if actual != *expected {
                return Err(ArtifactError::ArtifactIdentityConflict.into());
            }
            for stream in self.streams.values() {
                self.check_profile(stream, &actual, env)?;
                let spec = env
                    .specs
                    .get(&(stream.binding.instrument_slot, stream.binding.spec.version))
                    .cloned()
                    .ok_or(ModelError::UnknownDefinition)?;
                let (_, numeric) = env.instrument(spec.provenance, self.start.archive)?;
                if numeric != spec.numeric {
                    return Err(ArtifactError::ArtifactIdentityConflict.into());
                }
            }
        }
        match &input.value {
            Record::ArchiveStart(_) => return Err(EventError::RecordOrderError.into()),
            Record::InstrumentSpec(spec) => {
                let key = (spec.slot, spec.numeric.fields().reference.version);
                if env.specs.contains_key(&key) {
                    return Err(IdentityError::IdentityConflict.into());
                }
                for old in env.specs.values() {
                    if (old.slot == spec.slot)
                        != (old.numeric.fields().reference.instrument == spec.numeric.fields().reference.instrument)
                    {
                        return Err(IdentityError::IdentityConflict.into());
                    }
                }
                let body = env.instrument(spec.provenance, self.start.archive)?;
                if body != (spec.slot, spec.numeric.clone()) {
                    return Err(ArtifactError::ArtifactIdentityConflict.into());
                }
            }
            Record::StreamDefinition(definition) => {
                let bindings: Vec<_> = self.streams.values().map(|s| s.binding.clone()).collect();
                definition.binding.validate_registration(&bindings)?;
                let slot = definition.binding.instrument_slot;
                if self.active_specs.get(&slot) != Some(&definition.binding.spec.version) {
                    return Err(IdentityError::SpecMismatch.into());
                }
                let spec = env.specs.get(&(slot, definition.binding.spec.version)).ok_or(ModelError::UnknownDefinition)?;
                spec.numeric.fields().reference.ensure_same(&definition.binding.spec)?;
                let body = env.profile(definition.provenance, self.start.archive)?;
                Self::match_profile(&definition.binding, &body)?;
                if let Some((_, config)) = &self.config
                    && !body.supported_normalizers.contains(&config.normalizer_ref)
                {
                    return Err(ArtifactError::UnsupportedNormalizerBinding.into());
                }
            }
            Record::ConfigDefinition(definition) => {
                let body = env.config(definition.evidence, self.start.archive)?;
                if body.next != definition.next || body.provenance != definition.provenance_kind {
                    return Err(ArtifactError::ArtifactIdentityConflict.into());
                }
                definition.fields.validate_mirror(body.policy.fields)?;
                body.policy.validate(self.start.mode)?;
                for stream in self.streams.values() {
                    stream.loss.scope_change()?;
                }
                let mut timeline = self.timeline.clone();
                timeline.activate(
                    input.record_no,
                    definition.context.context,
                    definition.next,
                    body.normalizer_ref,
                    &env.prefix,
                )?;
            }
            Record::RawInput(raw) => {
                let stream = self.streams.get(&raw.stream).ok_or(ModelError::UnknownDefinition)?;
                if !env.specs.contains_key(&(stream.binding.instrument_slot, raw.tag.spec)) {
                    return Err(ModelError::UnknownDefinition);
                }
                if raw.tag == stream.binding.tag {
                    let id = RawFrameId { archive: self.start.archive, record: input.record_no };
                    let fact = env.normalizations.get(&id).ok_or(ArtifactError::ArtifactUnverified)?;
                    let source = &fact.frame.source;
                    if source.raw != id
                        || source.binding != stream.binding
                        || InputContext::Active(source.context) != raw.context.context
                        || source.received.unix_ns != raw.context.unix_ns
                        || source.received.monotonic.ns != raw.context.monotonic_ns
                        || source.received.monotonic.scope != self.clock()
                        || usize::try_from(fact.frame.raw_byte_len).ok() != Some(raw.bytes.len())
                    {
                        return Err(EventError::SourceMismatch.into());
                    }
                }
            }
            Record::Control(control) => {
                match &control.value {
                    Control::Verification(value) => self.check_raw_ref(value.raw, input.record_no, env)?,
                    Control::Warmup(value) => self.check_raw_ref(value.anchor, input.record_no, env)?,
                    Control::Freshness(value) => {
                        if let Some(basis) = value.basis {
                            self.check_raw_ref(basis, input.record_no, env)?;
                        }
                    }
                    Control::Recording(evidence) => {
                        let mut marks = self.recording.watermarks.clone();
                        marks.observe(input.record_no, self.last_record, evidence)?;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn check_profile(&self, stream: &StreamState, config: &ConfigBody, env: &mut ModelEnv) -> Result<()> {
        let profile = env.profile(stream.profile_ref, self.start.archive)?;
        Self::match_profile(&stream.binding, &profile)?;
        if !profile.supported_normalizers.contains(&config.normalizer_ref) {
            return Err(ArtifactError::UnsupportedNormalizerBinding.into());
        }
        Ok(())
    }

    fn match_profile(binding: &StreamBinding, profile: &FeedProfileBody) -> Result<()> {
        if profile.stream != binding.id
            || profile.instrument != binding.spec.instrument
            || profile.channel != binding.channel
            || profile.version != binding.feed_profile
        {
            return Err(ArtifactError::ArtifactIdentityConflict.into());
        }
        Ok(())
    }

    pub(super) fn check_raw_ref(&self, raw: RecordNo, before: RecordNo, env: &ModelEnv) -> Result<()> {
        if raw >= before {
            return Err(EventError::FutureCausalReference.into());
        }
        let id = RawFrameId { archive: self.start.archive, record: raw };
        if !env.raw_records.contains_key(&id) {
            return Err(EventError::MissingRawInput.into());
        }
        Ok(())
    }

    pub(super) fn dispatch(&mut self, input: &RecordFrame, env: &mut ModelEnv, out: &mut StepResult) -> Result<()> {
        let at = input.record_no;
        match &input.value {
            Record::InstrumentSpec(spec) => {
                self.active_specs.entry(spec.slot).or_insert(spec.numeric.fields().reference.version);
            }
            Record::StreamDefinition(definition) => {
                let binding = definition.binding.clone();
                let book = (binding.channel != Channel::Trades).then_some(BookValidity::NoSnapshot);
                self.transport
                    .entry((binding.connection_id, binding.tag.connection))
                    .or_insert(Transport::Unknown);
                self.streams.insert(binding.id, StreamState {
                    binding,
                    profile_ref: definition.provenance,
                    barrier: at.get(),
                    barrier_evaluation_ns: self.evaluation_ns,
                    freshness: Freshness::Unknown,
                    book,
                    pending: vec![],
                    anchor: None,
                    progress: 0,
                    witness: None,
                    last_valid_sample_ns: None,
                    last_applied_raw: None,
                    last_event_cursor: None,
                    last_batch_effects: vec![],
                    quiet: None,
                    loss: LossState::default(),
                    candidate: None,
                });
            }
            Record::ConfigDefinition(definition) => {
                let body = env.config(definition.evidence, self.start.archive)?;
                self.timeline.activate(at, definition.context.context, definition.next, body.normalizer_ref, &env.prefix)?;
                self.config = Some((definition.evidence, body));
                for stream in self.streams.values_mut() {
                    stream.invalidate(Fault::ContextChanged, at, self.evaluation_ns);
                }
            }
            Record::RawInput(raw) => self.raw(at, raw, env, out)?,
            Record::Control(control) => self.control(at, &control.value, env, out)?,
            Record::Gap(gap) => self.gap(at, gap, out)?,
            Record::ArchiveStart(_) => return Err(EventError::RecordOrderError.into()),
            // These are administrative records without a clock sample. Their
            // framing/seal-chain rules are independently enforced by WAL tests.
            Record::SegmentSeal(_) | Record::SegmentStart(_) | Record::ArchiveSeal(_) => {}
        }
        Ok(())
    }

    fn control(&mut self, at: RecordNo, value: &Control, env: &mut ModelEnv, out: &mut StepResult) -> Result<()> {
        match value {
            Control::Timer { stream, .. } => {
                if !self.streams.contains_key(stream) {
                    return Err(ModelError::UnknownDefinition);
                }
            }
            Control::Transport { connection, epoch, value } => {
                let affected: Vec<_> = self.streams.values()
                    .filter(|stream| stream.binding.connection_id == *connection)
                    .map(|stream| stream.binding.id).collect();
                if affected.is_empty() {
                    return Err(ModelError::UnknownDefinition);
                }
                let current = self.streams[&affected[0]].binding.tag.connection;
                if *epoch < current {
                    for id in affected {
                        self.diagnostic(at, id, DiagnosticCode::ObsoleteScope, out);
                    }
                    return Ok(());
                }
                if *epoch != current {
                    return Err(IdentityError::EpochMismatch.into());
                }
                self.transport.insert((*connection, *epoch), *value);
                if *value == Transport::Down {
                    for id in affected {
                        let state = self.streams.get_mut(&id).ok_or(ModelError::UnknownDefinition)?;
                        state.invalidate(Fault::TransportDown, at, self.evaluation_ns);
                    }
                }
            }
            Control::EpochAdvance { change, .. } => self.epoch(at, change)?,
            Control::SpecActivate { slot, expected, next } => {
                if self.active_specs.get(slot) != Some(expected) {
                    return Err(IdentityError::SpecMismatch.into());
                }
                let spec = env.specs.get(&(*slot, *next)).ok_or(ModelError::UnknownDefinition)?;
                for stream in self.streams.values().filter(|s| s.binding.instrument_slot == *slot) {
                    stream.loss.scope_change()?;
                    if stream.binding.spec.instrument != spec.numeric.fields().reference.instrument {
                        return Err(IdentityError::IdentityMismatch.into());
                    }
                }
                self.active_specs.insert(*slot, *next);
                for stream in self.streams.values_mut().filter(|s| s.binding.instrument_slot == *slot) {
                    stream.binding.spec = spec.numeric.fields().reference.clone();
                    stream.binding.tag.spec = *next;
                    stream.invalidate(Fault::ContextChanged, at, self.evaluation_ns);
                }
            }
            Control::Verification(value) => self.verification(at, value, env, out)?,
            Control::Warmup(value) => self.warmup(at, value, env, out)?,
            Control::Freshness(value) => self.freshness_evidence(at, value, env, out)?,
            Control::Recording(evidence) => {
                self.recording.watermarks.observe(at, self.last_record, evidence)?;
                self.recording.health = evidence.health;
                self.recording.reason = evidence.reason;
                self.recording.last_receipt = Some(at);
                if evidence.health != RecordingHealth::Healthy {
                    for stream in self.streams.values_mut() {
                        stream.revoke();
                    }
                }
            }
        }
        Ok(())
    }

    fn epoch(&mut self, at: RecordNo, change: &EpochChange) -> Result<()> {
        let ids: Vec<_> = self.streams.values().filter(|stream| match change {
            EpochChange::Connection { owner, .. } => stream.binding.connection_id == *owner,
            EpochChange::Subscription { owner, .. } => stream.binding.id == *owner,
            EpochChange::Book { owner, .. } => stream.binding.book_id == Some(*owner),
        }).map(|stream| stream.binding.id).collect();
        if ids.is_empty() {
            return Err(ModelError::UnknownDefinition);
        }
        for id in &ids {
            let state = &self.streams[id];
            state.loss.scope_change()?;
            match *change {
                EpochChange::Connection { expected, next, .. } => { state.binding.tag.connection.advance(expected, next)?; }
                EpochChange::Subscription { expected, next, .. } => { state.binding.tag.subscription.advance(expected, next)?; }
                EpochChange::Book { expected, next, .. } => {
                    state.binding.tag.book.ok_or(IdentityError::InvalidChannelBinding)?.advance(expected, next)?;
                }
            }
        }
        if let EpochChange::Connection { owner, expected, next } = *change {
            self.transport.remove(&(owner, expected));
            self.transport.insert((owner, next), Transport::Unknown);
        }
        for id in ids {
            let state = self.streams.get_mut(&id).ok_or(ModelError::UnknownDefinition)?;
            match *change {
                EpochChange::Connection { next, .. } => state.binding.tag.connection = next,
                EpochChange::Subscription { next, .. } => state.binding.tag.subscription = next,
                EpochChange::Book { next, .. } => state.binding.tag.book = Some(next),
            }
            state.new_epoch(at, self.evaluation_ns);
        }
        Ok(())
    }

    fn gap(&mut self, at: RecordNo, gap: &Gap, out: &mut StepResult) -> Result<()> {
        let targets = match &gap.scope {
            GapScope::ExplicitTargets(targets) => targets.clone(),
            GapScope::AllDeclaredStreams => self.streams.values().map(|s| GapTarget {
                stream: s.binding.id,
                tag: s.binding.tag,
                range: None,
                loss_count: None,
            }).collect(),
        };
        for target in targets {
            let state = self.streams.get_mut(&target.stream).ok_or(ModelError::UnknownDefinition)?;
            state.loss.gap(&target, gap.reason, at, state.binding.tag)?;
            if target.tag != state.binding.tag {
                self.diagnostic(at, target.stream, DiagnosticCode::ObsoleteScope, out);
                continue;
            }
            state.invalidate(Fault::Gap(gap.reason), at, self.evaluation_ns);
            if gap.reason == Reason::QueueOverflow {
                self.recording.health = RecordingHealth::Degraded;
                self.recording.reason = Reason::QueueOverflow;
            }
        }
        Ok(())
    }
}
