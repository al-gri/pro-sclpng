//! Typed policy tags and pure configuration guards from DataHealth section 2.1.
//! RecordingGate and WatermarkKind deliberately have different representations.

use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyError {
    Unsupported { field: &'static str, value: u8 },
    InvalidConfiguration { field: &'static str },
    InvalidPayload { field: &'static str, detail: &'static str },
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for PolicyError {}

type Result<T> = std::result::Result<T, PolicyError>;

macro_rules! tags {
    ($name:ident, $field:literal, $($variant:ident = $tag:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(u8)]
        pub enum $name {
            $($variant = $tag),+
        }

        impl $name {
            pub const fn tag(self) -> u8 {
                self as u8
            }
        }

        impl TryFrom<u8> for $name {
            type Error = PolicyError;

            fn try_from(value: u8) -> Result<Self> {
                match value {
                    $($tag => Ok(Self::$variant),)+
                    _ => Err(PolicyError::Unsupported { field: $field, value }),
                }
            }
        }
    };
}

tags!(
    SilenceRule,
    "Config.silence_rule",
    UnknownOnSilence = 1,
    StaleAfterDeadline = 2
);
tags!(
    RecordingGate,
    "Config.recording_gate",
    Written = 1,
    Flushed = 2,
    Durable = 3
);
tags!(
    WatermarkKind,
    "RecordingEvidence.watermark_kind",
    Accepted = 1,
    Appended = 2,
    Written = 3,
    Flushed = 4,
    Durable = 5
);
tags!(
    DurabilityMode,
    "ArchiveStart.durability_mode",
    Buffered = 1,
    GroupSynced = 2,
    SyncBeforePublish = 3
);

impl RecordingGate {
    /// Semantic strength, never an ordinal comparison against WatermarkKind.
    pub const fn covers(self, required: Self) -> bool {
        matches!(
            (self, required),
            (Self::Durable, _)
                | (Self::Flushed, Self::Flushed | Self::Written)
                | (Self::Written, Self::Written)
        )
    }
}

impl WatermarkKind {
    pub const fn achieved_gate(self) -> Option<RecordingGate> {
        match self {
            Self::Accepted | Self::Appended => None,
            Self::Written => Some(RecordingGate::Written),
            Self::Flushed => Some(RecordingGate::Flushed),
            Self::Durable => Some(RecordingGate::Durable),
        }
    }
}

impl DurabilityMode {
    pub fn validate_gate(self, gate: RecordingGate) -> Result<()> {
        if self != Self::Buffered && gate != RecordingGate::Durable {
            return Err(PolicyError::InvalidConfiguration {
                field: "Config.recording_gate",
            });
        }
        Ok(())
    }
}

/// The fields actually present in the WAL ConfigDefinition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicyFields {
    pub silence_rule: SilenceRule,
    pub freshness_deadline_ns: Option<u64>,
    pub warmup_min_updates: Option<u32>,
    pub warmup_min_elapsed_ns: Option<u64>,
    pub allow_quiet_with_proof: bool,
    pub require_two_sided_snapshot: bool,
    pub recording_gate: RecordingGate,
}

impl PolicyFields {
    pub fn validate(self, mode: DurabilityMode) -> Result<()> {
        if self.freshness_deadline_ns == Some(0)
            || (self.silence_rule == SilenceRule::StaleAfterDeadline
                && self.freshness_deadline_ns.is_none())
        {
            return Err(PolicyError::InvalidConfiguration {
                field: "Config.freshness_deadline_ns",
            });
        }
        if self.warmup_min_updates.is_none() && self.warmup_min_elapsed_ns.is_none() {
            return Err(PolicyError::InvalidConfiguration {
                field: "Config.warmup_thresholds",
            });
        }
        mode.validate_gate(self.recording_gate)
    }

    pub fn validate_mirror(self, descriptor: Self) -> Result<()> {
        let pairs = [
            (self.silence_rule == descriptor.silence_rule, "Config.silence_rule"),
            (self.recording_gate == descriptor.recording_gate, "Config.recording_gate"),
            (self.freshness_deadline_ns == descriptor.freshness_deadline_ns, "Config.freshness_deadline_ns"),
            (self.warmup_min_updates == descriptor.warmup_min_updates, "Config.warmup_min_updates"),
            (self.warmup_min_elapsed_ns == descriptor.warmup_min_elapsed_ns, "Config.warmup_min_elapsed_ns"),
            (self.allow_quiet_with_proof == descriptor.allow_quiet_with_proof, "Config.allow_quiet_with_proof"),
            (self.require_two_sided_snapshot == descriptor.require_two_sided_snapshot, "Config.require_two_sided_snapshot"),
        ];
        for (equal, field) in pairs {
            if !equal {
                return Err(PolicyError::InvalidPayload {
                    field,
                    detail: "PolicyRepresentationMismatch",
                });
            }
        }
        Ok(())
    }
}

/// Required external descriptor policy; this does not add fields to the WAL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthPolicy {
    pub fields: PolicyFields,
    pub pending_max_frames: u32,
    pub pending_max_raw_bytes: u64,
    pub pending_max_outputs: u32,
    pub pending_wait_ns: u64,
    pub quiet_max_lifetime_ns: Option<u64>,
}

impl HealthPolicy {
    pub fn validate(self, mode: DurabilityMode) -> Result<()> {
        self.fields.validate(mode)?;
        let bounds = [
            ((1..=256).contains(&self.pending_max_frames), "Config.pending_max_frames"),
            ((1..=16_777_216).contains(&self.pending_max_raw_bytes), "Config.pending_max_raw_bytes"),
            ((1..=65_536).contains(&self.pending_max_outputs), "Config.pending_max_outputs"),
            (self.pending_wait_ns > 0, "Config.pending_wait_ns"),
        ];
        for (valid, field) in bounds {
            if !valid {
                return Err(PolicyError::InvalidConfiguration { field });
            }
        }
        let quiet_valid = if self.fields.allow_quiet_with_proof {
            self.quiet_max_lifetime_ns.is_some_and(|n| n > 0)
        } else {
            self.quiet_max_lifetime_ns.is_none()
        };
        if !quiet_valid {
            return Err(PolicyError::InvalidConfiguration {
                field: "Config.quiet_max_lifetime_ns",
            });
        }
        Ok(())
    }
}
