use super::*;

impl HealthModel {
    pub(super) fn warmup(
        &mut self,
        at: RecordNo,
        wire: &WarmupEvidence,
        env: &mut ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let mut state = self
            .streams
            .remove(&wire.stream)
            .ok_or(ModelError::UnknownDefinition)?;
        if wire.anchor.get() <= state.barrier {
            self.diagnostic(at, wire.stream, DiagnosticCode::PreBarrier, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let context = self.context()?;
        let obsolete_body = env.warmups.get(&wire.proof).is_some_and(|body| {
            body.scope.context != context || body.scope.tag != state.binding.tag
        });
        if wire.tag != state.binding.tag || obsolete_body {
            self.diagnostic(at, wire.stream, DiagnosticCode::ObsoleteScope, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let config_ref = self.config.as_ref().ok_or(ModelError::UnknownDefinition)?.0;
        let body = env.warmup(
            wire.proof,
            config_ref,
            state.profile_ref,
            self.start.archive,
        )?;
        let mut required = vec![wire.anchor];
        if state.barrier > 0 {
            required.push(RecordNo::new(state.barrier)?);
        }
        let raw = RawFrameId {
            archive: self.start.archive,
            record: wire.anchor,
        };
        if let Some(proof) = env.first_proofs.get(&raw) {
            required.push(proof.record);
        }
        if let Some(last) = state.last_applied_raw {
            required.push(last);
            if let Some(proof) = env.first_proofs.get(&RawFrameId {
                archive: self.start.archive,
                record: last,
            }) {
                required.push(proof.record);
            }
        }
        env.check_basis(self.start.archive, at, &body.basis_records, &required)?;
        let matches_scope = body.scope == self.scope(&state)?
            && body.anchor == wire.anchor
            && body.update_count == wire.update_count
            && body.elapsed_ns == wire.elapsed_ns
            && state
                .anchor
                .as_ref()
                .is_some_and(|anchor| anchor.raw.record == wire.anchor);
        if matches_scope && state.witness.as_ref().is_some_and(|old| old.body == body) {
            self.diagnostic(at, wire.stream, DiagnosticCode::AlreadyApplied, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let policy = self.policy()?;
        let elapsed = state
            .anchor
            .as_ref()
            .and_then(|anchor| self.evaluation_ns.checked_sub(anchor.original_sample_ns));
        let valid = matches_scope
            && matches!(
                state.book,
                Some(BookValidity::Warming | BookValidity::Usable)
            )
            && body.observed_at_ns == self.evaluation_ns
            && Some(body.elapsed_ns) == elapsed
            && body.update_count == state.progress
            && policy
                .fields
                .warmup_min_updates
                .is_none_or(|n| state.progress >= n)
            && policy
                .fields
                .warmup_min_elapsed_ns
                .is_none_or(|n| body.elapsed_ns >= n);
        if valid {
            if state.book == Some(BookValidity::Warming) {
                state.book = Some(BookValidity::Usable);
                state.witness = Some(FrozenWitness {
                    record: at,
                    reference: wire.proof,
                    body,
                });
            }
            // Once Usable, progress and its first truthful witness stay frozen.
        } else {
            self.diagnostic(at, wire.stream, DiagnosticCode::WitnessMismatch, out);
        }
        self.streams.insert(wire.stream, state);
        Ok(())
    }

    pub(super) fn freshness_evidence(
        &mut self,
        at: RecordNo,
        wire: &FreshnessEvidence,
        env: &mut ModelEnv,
        out: &mut StepResult,
    ) -> Result<()> {
        let mut state = self
            .streams
            .remove(&wire.stream)
            .ok_or(ModelError::UnknownDefinition)?;
        let Some(basis) = wire.basis else {
            self.diagnostic(at, wire.stream, DiagnosticCode::MissingFreshnessBasis, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        };
        if basis.get() <= state.barrier {
            self.diagnostic(at, wire.stream, DiagnosticCode::PreBarrier, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let context = self.context()?;
        let obsolete_body = env.freshness.get(&wire.proof).is_some_and(|body| {
            body.scope.context != context || body.scope.tag != state.binding.tag
        });
        if wire.tag != state.binding.tag || obsolete_body {
            self.diagnostic(at, wire.stream, DiagnosticCode::ObsoleteScope, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let config_ref = self.config.as_ref().ok_or(ModelError::UnknownDefinition)?.0;
        let body = env.freshness(
            wire.proof,
            config_ref,
            state.profile_ref,
            self.start.archive,
        )?;
        let mut required = vec![basis];
        if state.barrier > 0 {
            required.push(RecordNo::new(state.barrier)?);
        }
        if let Some(anchor) = body.anchor {
            required.push(anchor);
        }
        env.check_basis(self.start.archive, at, &body.basis_records, &required)?;
        if body.scope.clock != self.clock() {
            state.freshness = Freshness::Unknown;
            state.quiet = None;
            self.diagnostic(at, wire.stream, DiagnosticCode::IncomparableClock, out);
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let raw_id = RawFrameId {
            archive: self.start.archive,
            record: basis,
        };
        let source = env.prefix.raw(raw_id).ok_or(EventError::MissingRawInput)?;
        let applied = state.last_applied_raw.is_some_and(|last| basis <= last)
            && (source.validate()? == FrameKind::Trades || env.first_proofs.contains_key(&raw_id));
        let scope_ok = body.scope == self.scope(&state)?
            && body.basis == basis
            && body.freshness == wire.freshness
            && source.source.binding == state.binding
            && applied;
        if !scope_ok {
            self.diagnostic(
                at,
                wire.stream,
                DiagnosticCode::FreshnessAssertionMismatch,
                out,
            );
            self.streams.insert(wire.stream, state);
            return Ok(());
        }
        let policy = self.policy()?;
        let sample = source.source.received.monotonic.ns.get();
        if wire.freshness == Freshness::QuietVerified {
            let error = self.apply_quiet(&mut state, body, sample, policy);
            if let Some(code) = error {
                self.diagnostic(at, wire.stream, code, out);
            }
        } else {
            let ordinary = ordinary_freshness(Some(sample), self.evaluation_ns, policy).0;
            if body.valid_from_ns.is_some()
                || body.valid_until_ns.is_some()
                || body.observed_at_ns != sample
                || body.freshness != ordinary
            {
                self.diagnostic(
                    at,
                    wire.stream,
                    DiagnosticCode::FreshnessAssertionMismatch,
                    out,
                );
            }
            // Ordinary evidence is an assertion, never a command to overwrite
            // the freshness of a later already-applied sample.
        }
        self.streams.insert(wire.stream, state);
        Ok(())
    }

    fn apply_quiet(
        &self,
        state: &mut StreamState,
        body: FreshnessBody,
        sample: u64,
        policy: HealthPolicy,
    ) -> Option<DiagnosticCode> {
        if !policy.fields.allow_quiet_with_proof {
            return Some(DiagnosticCode::QuietPolicyDenied);
        }
        let anchor = state.anchor.as_ref().map(|anchor| anchor.raw.record);
        if body.anchor != anchor || (state.book.is_some() && anchor.is_none()) {
            return Some(DiagnosticCode::FreshnessAssertionMismatch);
        }
        let (Some(from), Some(until), Some(max_lifetime)) = (
            body.valid_from_ns,
            body.valid_until_ns,
            policy.quiet_max_lifetime_ns,
        ) else {
            return Some(DiagnosticCode::InvalidQuietBounds);
        };
        if from < body.observed_at_ns || until <= from || body.observed_at_ns < sample {
            return Some(DiagnosticCode::InvalidQuietBounds);
        }
        let Some(policy_end) = body.observed_at_ns.checked_add(max_lifetime) else {
            return Some(DiagnosticCode::InvalidQuietBounds);
        };
        let expires_ns = until.min(policy_end);
        if self.evaluation_ns < from {
            return Some(DiagnosticCode::QuietNotYetValid);
        }
        if self.evaluation_ns >= expires_ns {
            return Some(DiagnosticCode::QuietExpired);
        }
        state.freshness = Freshness::QuietVerified;
        state.quiet = Some(QuietWitness { body, expires_ns });
        None
    }
}
