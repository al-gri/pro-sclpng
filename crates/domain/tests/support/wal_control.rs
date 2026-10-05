//! Exact control and GAP layouts for the test-only WAL codec.

use domain::identity::*;
use domain::record::*;

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked};

pub fn decode_control(r: &mut Reader<'_>) -> Result<Control> {
    let offset = r.offset();
    let tag = r.u8()?;
    Ok(match tag {
        1 => Control::Timer {
            stream: r.stream()?,
            timer_id: r.u64()?,
            deadline_ns: r.u64()?,
        },
        2 => Control::Transport {
            connection: r.connection()?,
            epoch: r.connection_epoch()?,
            value: r.enum_tag("Transport.liveness")?,
        },
        3 => {
            let offset = r.offset();
            let scope = r.u8()?;
            let change = match scope {
                1 => EpochChange::Connection {
                    owner: r.connection()?,
                    expected: r.connection_epoch()?,
                    next: r.connection_epoch()?,
                },
                2 => EpochChange::Subscription {
                    owner: r.stream()?,
                    expected: r.subscription_epoch()?,
                    next: r.subscription_epoch()?,
                },
                3 => EpochChange::Book {
                    owner: r.book()?,
                    expected: r.book_epoch()?,
                    next: r.book_epoch()?,
                },
                _ => {
                    return Err(Error::new(
                        offset,
                        ErrorKind::Unsupported {
                            field: "EpochAdvance.scope",
                            value: scope.into(),
                        },
                    ));
                }
            };
            Control::EpochAdvance {
                change,
                reason: r.enum_tag("reason")?,
            }
        }
        4 => Control::SpecActivate {
            slot: r.slot()?,
            expected: r.spec_version()?,
            next: r.spec_version()?,
        },
        5 => Control::Verification(VerificationEvidence {
            stream: r.stream()?,
            tag: r.epoch_tag()?,
            raw: r.record_no()?,
            kind: r.enum_tag("Verification.evidence_kind")?,
            profile: r.profile_version()?,
            proof: r.artifact()?,
        }),
        6 => Control::Warmup(WarmupEvidence {
            stream: r.stream()?,
            tag: r.epoch_tag()?,
            anchor: r.record_no()?,
            update_count: r.u32()?,
            elapsed_ns: r.u64()?,
            proof: r.artifact()?,
        }),
        7 => Control::Freshness(FreshnessEvidence {
            stream: r.stream()?,
            tag: r.epoch_tag()?,
            freshness: r.enum_tag("FreshnessEvidence.freshness")?,
            basis: r.option(Reader::record_no)?,
            proof: r.artifact()?,
        }),
        8 => Control::Recording(RecordingEvidence {
            health: r.enum_tag("RecordingEvidence.health")?,
            kind: r.enum_tag("RecordingEvidence.watermark_kind")?,
            through: r.option(Reader::record_no)?,
            reason: r.enum_tag("reason")?,
        }),
        _ => {
            return Err(Error::new(
                offset,
                ErrorKind::Unsupported {
                    field: "control_tag",
                    value: tag.into(),
                },
            ));
        }
    })
}

pub fn decode_gap(r: &mut Reader<'_>, context: WireContext) -> Result<Gap> {
    let scope_offset = r.offset();
    let scope_tag = r.u8()?;
    // This check intentionally occurs before reading reason/count or touching
    // accounting. V2 reversed bytes must fail here at frame offset56.
    if scope_tag != 1 && scope_tag != 2 {
        return Err(Error::new(
            scope_offset,
            ErrorKind::Unsupported {
                field: "Gap.scope_kind",
                value: scope_tag.into(),
            },
        ));
    }
    let reason = r.enum_tag("reason")?;
    let count_offset = r.offset();
    let count = usize::from(r.u16()?);
    let scope = if scope_tag == 2 {
        if count != 0 {
            return Err(Error::new(
                count_offset,
                ErrorKind::InvalidPayload("Gap.target_count"),
            ));
        }
        GapScope::AllDeclaredStreams
    } else {
        if count == 0 || count > 256 {
            return Err(Error::new(
                count_offset,
                ErrorKind::InvalidPayload("Gap.target_count"),
            ));
        }
        // Smallest target: StreamId4 + spec4/connection8/subscription8/bookOpt1
        // + three absent option tags3 = 28 bytes. Book targets are larger.
        r.count(count, 28, 256)?;
        let mut targets = Vec::with_capacity(count);
        for _ in 0..count {
            let stream = r.stream()?;
            let tag = r.epoch_tag()?;
            let offset = r.offset();
            let first = r.option(Reader::attempt)?;
            let last = r.option(Reader::attempt)?;
            let range = match (first, last) {
                (None, None) => None,
                (Some(first), Some(last)) => Some((first, last)),
                _ => return Err(Error::new(offset, ErrorKind::InvalidPayload("Gap.range"))),
            };
            let loss_count = r.option(Reader::u64)?;
            targets.push(GapTarget {
                stream,
                tag,
                range,
                loss_count,
            });
        }
        GapScope::ExplicitTargets(targets)
    };
    let gap = Gap {
        context,
        scope,
        reason,
    };
    checked(scope_offset, gap.validate())?;
    Ok(gap)
}

pub fn encode_control(w: &mut Writer, value: &Control) -> Result<()> {
    w.u8(value.tag())?;
    match value {
        Control::Timer {
            stream,
            timer_id,
            deadline_ns,
        } => {
            w.u32(stream.get())?;
            w.u64(*timer_id)?;
            w.u64(*deadline_ns)?;
        }
        Control::Transport {
            connection,
            epoch,
            value,
        } => {
            w.u32(connection.get())?;
            w.u64(epoch.get())?;
            w.u8(value.tag())?;
        }
        Control::EpochAdvance { change, reason } => {
            let (scope, owner, expected, next) = change.wire_parts();
            w.u8(scope)?;
            w.u32(owner)?;
            w.u64(expected)?;
            w.u64(next)?;
            w.u8(reason.tag())?;
        }
        Control::SpecActivate {
            slot,
            expected,
            next,
        } => {
            w.u32(slot.get())?;
            w.u32(expected.get())?;
            w.u32(next.get())?;
        }
        Control::Verification(v) => {
            w.u32(v.stream.get())?;
            w.epoch_tag(v.tag)?;
            w.u64(v.raw.get())?;
            w.u8(v.kind.tag())?;
            w.u32(v.profile.get())?;
            w.artifact(v.proof)?;
        }
        Control::Warmup(v) => {
            w.u32(v.stream.get())?;
            w.epoch_tag(v.tag)?;
            w.u64(v.anchor.get())?;
            w.u32(v.update_count)?;
            w.u64(v.elapsed_ns)?;
            w.artifact(v.proof)?;
        }
        Control::Freshness(v) => {
            w.u32(v.stream.get())?;
            w.epoch_tag(v.tag)?;
            w.u8(v.freshness.tag())?;
            w.option(v.basis, |w, record| w.u64(record.get()))?;
            w.artifact(v.proof)?;
        }
        Control::Recording(v) => {
            w.u8(v.health.tag())?;
            w.u8(v.kind.tag())?;
            w.option(v.through, |w, record| w.u64(record.get()))?;
            w.u8(v.reason.tag())?;
        }
    }
    Ok(())
}

pub fn encode_gap(w: &mut Writer, value: &Gap) -> Result<()> {
    w.u8(value.scope.tag())?;
    w.u8(value.reason.tag())?;
    match &value.scope {
        GapScope::AllDeclaredStreams => w.u16(0)?,
        GapScope::ExplicitTargets(targets) => {
            let count =
                u16::try_from(targets.len()).map_err(|_| Error::new(58, ErrorKind::LengthError))?;
            w.u16(count)?;
            for target in targets {
                w.u32(target.stream.get())?;
                w.epoch_tag(target.tag)?;
                w.option(target.range.map(|v| v.0), |w, v: CaptureAttemptNo| {
                    w.u64(v.get())
                })?;
                w.option(target.range.map(|v| v.1), |w, v: CaptureAttemptNo| {
                    w.u64(v.get())
                })?;
                w.option(target.loss_count, Writer::u64)?;
            }
        }
    }
    Ok(())
}
