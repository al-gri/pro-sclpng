//! Single-frame decode-before-transition adapter for byte-level state vectors.
//! Segment-chain validation remains the separate memory recovery helper.

use domain::record::RecordError;

use super::binary::{Error, ErrorKind};
use super::health::{HealthModel, ModelError, StepResult};
use super::model_env::ModelEnv;
use super::wal::decode_exact;
use super::wal_recovery::RecoveryFault;

pub fn apply_exact(
    bytes: &[u8],
    model: &mut HealthModel,
    env: &mut ModelEnv,
) -> Result<StepResult, RecoveryFault> {
    if model.blocked.is_some() {
        return Err(RecoveryFault::Semantic(ModelError::AlreadyBlocked));
    }
    let active = model.timeline.active().is_some();
    let frame = match decode_exact(bytes, active, &env.specs) {
        Ok(frame) => frame,
        Err(error) => {
            model.blocked = Some(binary_block(&error));
            return Err(RecoveryFault::Binary(error));
        }
    };
    model.step(&frame, env).map_err(RecoveryFault::Semantic)
}

pub fn binary_block(error: &Error) -> ModelError {
    match error.kind {
        ErrorKind::Unsupported { field, value } => {
            ModelError::Record(RecordError::Unsupported { field, value })
        }
        _ => ModelError::Record(RecordError::InvalidPayload("WAL.frame")),
    }
}
